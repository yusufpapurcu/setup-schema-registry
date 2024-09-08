package main

import (
	"embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed schemas
var schemaDir embed.FS

// This script will try and connect to the topic leader within the timeout, else return an os.Exit(1).
// We connect to the topic leader to ensure a leadership election succeeds and the topic is ready to be consumed.
func main() {
	registryEndpoint := GetEnvarOrFatal("KAFKA_SCHEMA_REGISTRY_ENDPOINT")
	client := &RegistryClient{registryURL: registryEndpoint}
	timeout := time.After(2 * time.Minute)

	var schemas []schema
	for _, i := range []int{2, 1, 0} {
		dirEntry, err := schemaDir.ReadDir(fmt.Sprintf("schemas/%d", i))
		if err != nil {
			log.Fatal(err.Error())
		}

		for _, entry := range dirEntry {
			f, err := schemaDir.Open(fmt.Sprintf("schemas/%d/%s/schema.json", i, entry.Name()))
			if err != nil {
				log.Fatal(err.Error())
			}

			res, err := io.ReadAll(f)
			if err != nil {
				log.Fatal(err.Error())
			}

			schemas = append(schemas, schema{
				schemaName: entry.Name(),
				schemaBody: string(res),
			})

			f.Close()
		}
	}

OUTSIDE:
	for {
		select {
		case <-timeout:
			log.Fatal("timeout trying to setup registry")
		default:
			time.Sleep(10 * time.Second)

			_, err := http.Get(fmt.Sprintf("%s/subjects", registryEndpoint))
			if err != nil {
				log.Printf("failed to dial registry: %s", err)

				continue
			}
			for _, schema := range schemas {
				err = client.AddSchema(schema.schemaName, schema.schemaBody)
				if err != nil {
					log.Println("failed to register", schema.schemaName, ":", err)

					continue OUTSIDE
				}

				log.Printf("%s successfully sent to the registry\n", schema.schemaName)
			}

			log.Println("all schemas successfully sent to the registry")
			return
		}
	}
}

func GetEnvarOrFatal(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("missing %s environment variable", key)
	}
	return value
}

type RegistryClient struct {
	registryURL string
}

func (r RegistryClient) AddSchema(subjectName, schema string) error {
	res, err := http.Post(r.registryURL+"/subjects/"+subjectName+"/versions", "application/json", strings.NewReader(schema))
	if err != nil || res.StatusCode != 200 {
		if body, err := io.ReadAll(res.Body); err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		} else {
			log.Printf("response body: %s\n", string(body))
		}

		log.Printf("response code:%d\n", res.StatusCode)
		return fmt.Errorf("failed to submit schema %q: %v\n", subjectName, err)
	}
	return nil
}

type schema struct {
	schemaName string
	schemaBody string
}
