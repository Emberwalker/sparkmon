package main

import (
	"encoding/json"
	"fmt"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	host := "http://localhost:4040"
	if len(os.Args) > 1 {
		host = os.Args[1]
	}

	state := computeState(host)
	termWidth, termHeight := ui.TerminalDimensions()
	render(termWidth, termHeight, state)

	ticker := time.Tick(5 * time.Second)
	uiEvents := ui.PollEvents()

	for {
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "r":
				state = computeState(host)
				render(termWidth, termHeight, state)
			case "q", "<C-c>":
				return
			case "<Resize>":
				payload := e.Payload.(ui.Resize)
				termWidth = payload.Width
				termHeight = payload.Height
				render(termWidth, termHeight, state)
			}
		case <-ticker:
			state = computeState(host)
			render(termWidth, termHeight, state)
		}
	}
}

func render(width, height int, state State) {
	drawables := make([]ui.Drawable, 0)
	currentHeight := 0

	// Add host header
	clusterInfo := widgets.NewParagraph()
	clusterInfo.Border = false
	clusterInfo.TextStyle.Modifier = ui.ModifierBold
	clusterInfo.Text = fmt.Sprintf("Running against Spark on %v", state.Host)
	clusterInfo.SetRect(0, 0, width, currentHeight+1)
	drawables = append(drawables, clusterInfo)

	currentHeight += 2

	for _, app := range state.Apps {
		// Used for working out length of labels vs progress bars
		labelWidth := width / 2

		// Start at 1 for root block top border
		currentInternalHeight := 1

		// Root block
		root := ui.NewBlock()
		root.Border = true
		root.Title = fmt.Sprintf("%v (%v)", app.App.Name, app.App.Id)
		root.TitleStyle.Modifier = ui.ModifierBold + ui.ModifierUnderline
		drawables = append(drawables, root)

		for _, job := range app.Jobs {
			// Job block, under the root
			jobBlock := ui.NewBlock()
			jobBlock.Border = true
			jobBlock.Title = fmt.Sprintf("%v (Stage %v)", job.Job.Name, job.Job.Index)
			jobBlock.TitleStyle.Modifier = ui.ModifierClear
			initialHeight := currentInternalHeight
			currentInternalHeight += 1
			drawables = append(drawables, jobBlock)

			for _, stage := range job.Stages {
				// Stage label + ID
				label := widgets.NewParagraph()
				label.Border = false
				labelText := fmt.Sprintf("%v %v: %v", stage.Index, stage.Status, stage.Name)
				if len(labelText) > labelWidth {
					labelText = labelText[:labelWidth-3] + "..."
				}
				label.Text = labelText
				label.SetRect(2, currentHeight+currentInternalHeight, labelWidth, currentHeight+currentInternalHeight+1)

				// Stage progress bar
				bar := widgets.NewGauge()
				bar.Border = false
				if stage.ActiveTasks == 0 {
					bar.Label = fmt.Sprintf("%v/%v", stage.CompletedTasks, stage.Tasks)
				} else {
					bar.Label = fmt.Sprintf("%v/%v (%v active)", stage.CompletedTasks, stage.Tasks, stage.ActiveTasks)
				}
				bar.Percent = int(math.Ceil((float64(stage.CompletedTasks) / float64(stage.Tasks)) * 100))
				bar.SetRect(labelWidth, currentHeight+currentInternalHeight, width-2, currentHeight+currentInternalHeight+1)

				currentInternalHeight += 1

				// The first non-Apache stacktrace entry from details
				detailsText := "<spark internal>"
				for _, text := range strings.Split(stage.Details, "\n") {
					if !strings.HasPrefix(text, "org.apache") {
						detailsText = text
						break
					}
				}
				details := widgets.NewParagraph()
				details.Border = false
				details.TextStyle.Modifier = ui.ModifierClear
				details.TextStyle.Fg = ui.ColorRed
				details.Text = detailsText
				details.SetRect(2, currentHeight+currentInternalHeight, width-2, currentHeight+currentInternalHeight+1)

				currentInternalHeight += 1

				drawables = append(drawables, label, bar, details)
			}

			currentInternalHeight += 1
			// -1 width to fit in root
			jobBlock.SetRect(1, currentHeight+initialHeight, width-1, currentHeight+currentInternalHeight)
		}

		// +1 for root block bottom border
		root.SetRect(0, currentHeight, width, currentHeight+currentInternalHeight+1)
		currentHeight += currentInternalHeight + 1
	}

	ui.Clear()
	ui.Render(drawables...)
}

func computeState(host string) State {
	apps := getApplications(host)
	appsEnriched := make([]EnrichedApplication, len(apps))
	for i, app := range apps {
		jobs := getJobs(host, app)
		stages := getStages(host, app)
		stagesMap := make(map[int]Stage)
		for _, stage := range stages {
			stagesMap[stage.Index] = stage
		}

		jobsEnriched := make([]EnrichedJob, len(jobs))
		for j, job := range jobs {
			jobStages := make([]Stage, len(job.Stages))
			for k, jobStage := range job.Stages {
				jobStages[k] = stagesMap[jobStage]
			}

			jobsEnriched[j] = EnrichedJob{
				Job:    job,
				Stages: jobStages,
			}
		}

		appsEnriched[i] = EnrichedApplication{
			App:  app,
			Jobs: jobsEnriched,
		}
	}

	return State{
		Apps: appsEnriched,
		Host: host,
	}
}

func getApplications(host string) []ApplicationIdAndName {
	apps := make([]ApplicationIdAndName, 0)
	readApiEndpoint(host, "/api/v1/applications", &apps)
	return apps
}

func getJobs(host string, app ApplicationIdAndName) []Job {
	jobs := make([]Job, 0)
	endpoint := fmt.Sprintf("/api/v1/applications/%v/jobs", app.Id)
	readApiEndpoint(host, endpoint, &jobs)
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].Index < jobs[j].Index })
	return jobs
}

func getStages(host string, app ApplicationIdAndName) []Stage {
	stages := make([]Stage, 0)
	endpoint := fmt.Sprintf("/api/v1/applications/%v/stages", app.Id)
	readApiEndpoint(host, endpoint, &stages)
	sort.Slice(stages, func(i, j int) bool { return stages[i].Index < stages[j].Index })
	return stages
}

func readApiEndpoint(host string, endpoint string, out interface{}) {
	resp, err := http.Get(host + endpoint)
	if err != nil {
		log.Fatalf("Failed to GET endpoint %v: %v", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read body for %v: %v", endpoint, err)
	}

	err = json.Unmarshal(body, &out)
	if err != nil {
		log.Fatalf("Failed to parse applications response for %v: %v", endpoint, err)
	}
}

type ApplicationIdAndName struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Job struct {
	Index  int    `json:"jobId"`
	Name   string `json:"name"`
	Stages []int  `json:"stageIds"`
	Status string `json:"status"`
}

type Stage struct {
	Index          int    `json:"stageId"`
	Name           string `json:"name"`
	Details        string `json:"details"`
	Status         string `json:"status"`
	Tasks          int    `json:"numTasks"`
	ActiveTasks    int    `json:"numActiveTasks"`
	CompletedTasks int    `json:"numCompleteTasks"`
	FailedTasks    int    `json:"numFailedTasks"`
	KilledTasks    int    `json:"numKilledTasks"`
}

type State struct {
	Apps []EnrichedApplication
	Host string
}

type EnrichedApplication struct {
	App  ApplicationIdAndName
	Jobs []EnrichedJob
}

type EnrichedJob struct {
	Job    Job
	Stages []Stage
}
