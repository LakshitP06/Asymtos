package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogEntry struct {
	Severity  string    `json:"severity"`
	Source    string    `json:"source"`
	Content   string    `json:"content"`
	Filename  string    `json:"filename"`
	Timestamp time.Time `json:"timestamp"`
}

type SeverityCount struct {
	Time  string `bson:"time"`
	Count int    `bson:"count"`
}

type SeverityData struct {
	Intervals []SeverityCount `bson:"intervals"`
}

func main() {
	// Specify the MongoDB connection string
	clientOptions := options.Client().ApplyURI("mongodb+srv://admin:password%4011@cluster0.xgmrkw2.mongodb.net/")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Create a MongoDB collection
	db := client.Database("asymtos")
	collection := db.Collection("parsed_logs")

	// Create a map to store the aggregated severity data
	severityData := make(map[string]SeverityData)

	// Define the root directory
	rootDir := "D:\\GOLANG\\rohit"

	for {
		currentTime := time.Now()
		timeFormatted := currentTime.Format("15:04")

		var startTimeStr string
		var endTimeStr string
		endTimeStr = timeFormatted

		twoHoursAgo := currentTime.Add(-2 * time.Hour)
		timeFormattedEnd := twoHoursAgo.Format("15:04")
		startTimeStr = timeFormattedEnd

		// Parse the user input into time.Time objects
		startTime, err := time.Parse("15:04", startTimeStr)
		if err != nil {
			log.Fatalf("Error parsing start time: %v", err)
		}

		endTime, err := time.Parse("15:04", endTimeStr)
		if err != nil {
			log.Fatalf("Error parsing end time: %v", err)
		}

		startTime = startTime.Truncate(10 * time.Minute)
		endTime = endTime.Truncate(10 * time.Minute)

		// Initialize severityData with all severity levels and intervals
		for _, severity := range []string{"debug", "err", "notice", "info", "error", "warning"} {
			severityData[severity] = SeverityData{
				Intervals: initializeIntervals(startTime, endTime),
			}
		}

		// Create a map to store the processed file paths
		processedFiles := make(map[string]bool)

		// Walk through all directories and files in the root directory
		err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing path %s: %s\n", path, err)
				return err
			}
			// Check if it's a directory
			if info.IsDir() {
				// Check if the folder name is 'logs'
				if info.Name() == "logs" {
					files, err := os.ReadDir(path)
					if err != nil {
						log.Printf("Error reading directory %s: %s\n", path, err)
						return err
					}
					for _, file := range files {
						filePath := filepath.Join(path, file.Name())

						// Check if the file has already been processed
						if !processedFiles[filePath] {
							processFile(filePath, severityData, startTime, endTime)
							processedFiles[filePath] = true
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Error walking through directories: %s\n", err)
		}

		// Update the MongoDB document with the aggregated severity data
		filter := bson.M{}
		update := bson.M{
			"$set": bson.M{"intervals": severityData},
		}
		_, err = collection.UpdateOne(context.Background(), filter, update, options.Update().SetUpsert(true))
		if err != nil {
			log.Fatalf("Error updating MongoDB document: %v", err)
		}

		fmt.Println("Data updated in MongoDB successfully.")

		time.Sleep(1 * time.Minute)
	}
}

func initializeIntervals(startTime, endTime time.Time) []SeverityCount {
	intervals := make([]SeverityCount, 0)
	currentTime := startTime

	// Create intervals from startTime to endTime with a count of 0
	for currentTime.Before(endTime) {
		intervalTime := currentTime.Format("15:04")
		intervals = append(intervals, SeverityCount{
			Time:  intervalTime,
			Count: 0,
		})
		currentTime = currentTime.Add(10 * time.Minute)
	}

	return intervals
}

func processFile(filePath string, severityData map[string]SeverityData, startTime, endTime time.Time) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening file %s: %s\n", filePath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	const maxTokenSize = 10 * 1024 * 1024 // Increase the maximum token size to 10 MB
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.TrimSpace(line) == "" {
			continue
		}

		// Split the line by "}{"
		objectStrings := strings.Split(line, "}{")
		for _, objStr := range objectStrings {
			// Reconstruct the JSON object, accounting for missing commas
			if strings.Index(objStr, "{") != 0 {
				objStr = "{" + objStr
			}
			if strings.LastIndex(objStr, "}") != len(objStr)-1 {
				objStr = objStr + "}"
			}

			var entry LogEntry
			err := json.Unmarshal([]byte(objStr), &entry)
			if err != nil {
				fmt.Printf("Error parsing log entry: %s\n", err)
				continue
			}

			// Skip entries with an empty severity type
			if entry.Severity == "" {
				continue
			}

			// Calculate the 10-minute interval based on the timestamp
			interval := entry.Timestamp.Truncate(10 * time.Minute)
			intervalTime := interval.Format("15:04") // Format time as HH:mm

			// Check if the interval is within the specified start and end times
			if intervalTime >= startTime.Format("15:04") && intervalTime <= endTime.Format("15:04") {
				// Update the count for the current severity and interval
				data, ok := severityData[entry.Severity]
				if !ok {
					data = SeverityData{}
				}

				// Find the index of the interval in the data
				index := -1
				for i, s := range data.Intervals {
					if s.Time == intervalTime {
						index = i
						break
					}
				}

				// If the interval doesn't exist in the data, add it
				if index == -1 {
					data.Intervals = append(data.Intervals, SeverityCount{
						Time:  intervalTime,
						Count: 0,
					})
					index = len(data.Intervals) - 1
				}

				// Update the count
				data.Intervals[index].Count++
				severityData[entry.Severity] = data
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading file %s: %s\n", filePath, err)
	}
}
