package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var redisClient *redis.Client

type Handlers map[string]func(ctx context.Context, data map[string]any) error

type Worker struct {
	instance *redis.Client
	queue    string
	handlers Handlers
}

type GetKeyFromQueueResponse struct {
	Name        *string
	Data        *string
	IsProcessed bool
	IsNotFound  bool
}

func RegisterInstance(connection *redis.Client) {
	redisClient = connection
}

func CreateKey(queue string, key string) string {
	return fmt.Sprintf("bull:%s:%s", queue, key)
}

func GetValueByKey(key string) GetKeyFromQueueResponse {

	job, err := redisClient.HGetAll(ctx, key).Result()

	if err != nil {
		fmt.Println(err)
	}

	if job["name"] == "" {
		return GetKeyFromQueueResponse{
			IsNotFound:  true,
			IsProcessed: false,
		}
	}

	if job["processedOn"] != "" {
		return GetKeyFromQueueResponse{
			IsProcessed: true,
			IsNotFound:  false,
		}
	}

	value, err := redisClient.HGet(ctx, key, "data").Result()

	if err != nil {
		fmt.Println(err)
	}

	jobName := job["name"]

	return GetKeyFromQueueResponse{
		Data:        &value,
		Name:        &jobName,
		IsProcessed: false,
		IsNotFound:  false,
	}
}

func CreateLastReadKey(queue string) string {
	return fmt.Sprintf("bull:%s:completed", queue)
}

func SetLastRead(queue string, key int) {

	fullKeyJob := fmt.Sprintf("bull:%s:%d", queue, key)

	redisClient.HSet(ctx, fullKeyJob,
		"finishedOn", time.Now().UnixMilli(),
		"returnvalue", `null`,
	)

	score := float64(time.Now().UnixMilli())

	fullKeyLastRead := CreateLastReadKey(queue)

	redisClient.ZAdd(ctx, fullKeyLastRead, redis.Z{
		Score:  score,
		Member: key,
	})
}

func SetJobFinished(queue string, key int) {
	fullKeyJob := fmt.Sprintf("bull:%s:%d", queue, key)

	redisClient.HSet(ctx, fullKeyJob,
		"finishedOn", time.Now().UnixMilli(),
		"returnvalue", `null`,
	)
}

func GetLastRead(queue string) int {

	fullKey := CreateLastReadKey(queue)

	value, err := redisClient.ZRange(ctx, fullKey, 0, -1).Result()

	if err != nil {
		fmt.Println(err)
	}

	if len(value) == 0 {
		return 0
	}

	integerValue, err := strconv.Atoi(value[len(value)-1])

	if err != nil {
		fmt.Println(err)
	}

	return integerValue
}

func StartWorker(worker Worker) {
	log.Printf("Worker to consume %s (%s) started...", worker.queue, worker.instance.Options().Addr)

	RegisterInstance(worker.instance)

	for {
		lastRead := GetLastRead(worker.queue)

		nextToRead := lastRead + 1
		key := CreateKey(worker.queue, strconv.Itoa(nextToRead))

		jobToProcess := GetValueByKey(key)

		if jobToProcess.IsProcessed {
			log.Printf("No jobs to process at %s...", worker.queue)
			SetLastRead(worker.queue, nextToRead)
		}

		if !jobToProcess.IsNotFound {

			if jobToProcess.Name != nil {
				jobName := *jobToProcess.Name
				jobData := *jobToProcess.Data

				log.Printf("Dispatching... %s", jobName)

				handler := worker.handlers[jobName]

				var data map[string]any
				if err := json.Unmarshal([]byte(jobData), &data); err != nil {
					continue
				}

				handler(ctx, data)

				SetLastRead(worker.queue, nextToRead)
				SetJobFinished(worker.queue, nextToRead)
			}

		}

		time.Sleep(1 * time.Second)
	}
}
