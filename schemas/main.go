package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const basePath = ""
const baseURL = "https://hostname.schemaregistry.xxx/"

func main() {
	versions := readEnvFile("../.schema-versions.env")
	envVariables := readEnvFile("../.local.env")

	schemas := make(map[string]Schema)
	for _, tn := range append(envVariables, versions...) {
		tn = strings.TrimSpace(tn)
		if tn == "" {
			continue
		}

		key, value := strings.Split(tn, "=")[0], strings.Split(tn, "=")[1]
		if strings.HasPrefix(key, "KAFKA_SCHEMA_REGISTRY") {
			if strings.Contains(key, "ENDPOINT") {
				continue
			}

			// versions always after schemaName
			if strings.Contains(key, "VERSION") {
				key = strings.ReplaceAll(key, "_VERSION", "_KEY")
				val, err := strconv.Atoi(value)
				if err != nil {
					log.Fatal("invalid version" + value)
				}

				existing := schemas[key]
				existing.Version = val
				schemas[key] = existing
				continue
			}

			schemas[key] = Schema{Name: value, Version: 1}
		}
	}

	depth := 5
	fmt.Println("Cleaning up existing folders, up to depth:", depth)
	for i := range make([]int, depth) {
		err := os.RemoveAll(basePath + fmt.Sprint(i))
		if err != nil {
			fmt.Println("failed to remove folders, folder:", i)
			return
		}
	}

	for key, schema := range schemas {
		fmt.Println(key, schema.Name, schema.Version)

		createSchema(schema.Name, schema.Version, 0)
	}
}

type Schema struct {
	Name    string
	Version int
}

func createSchema(name string, version, depth int) {
	err := os.MkdirAll(basePath+fmt.Sprint(depth)+"/"+name, 0o755) // The owner can read, write, execute. Everyone else can read and execute but not modify the file.
	if err != nil {
		fmt.Println("==================================================")
		log.Fatalf("failed to create dir for %s: \n%s", name, err.Error())
	}

	resp, err := http.Get(baseURL + "subjects/" + name + "/versions/" + fmt.Sprint(version))
	if err != nil {
		log.Fatal(err.Error())
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	references := gjson.Get(string(raw), "references")
	references.ForEach(func(key, value gjson.Result) bool {
		name := value.Get("subject").String()
		version := value.Get("version").Int()
		createSchema(name, int(version), depth+1)

		return true
	})

	// Setting all versions to 1 since local don't support versions
	versionFix, err := sjson.Set(string(raw), "references.#.version", 1)
	if err != nil {
		log.Fatal(err.Error())
	}

	compatibility_level, err := sjson.Set(versionFix, "compatibility_level", "FULL")
	if err != nil {
		log.Fatal(err.Error())
	}

	subjectName, err := sjson.Set(compatibility_level, "subject_name", name)
	if err != nil {
		log.Fatal(err.Error())
	}

	noSubject, err := sjson.Delete(subjectName, "subject")
	if err != nil {
		log.Fatal(err.Error())
	}

	noVersion, err := sjson.Delete(noSubject, "version")
	if err != nil {
		log.Fatal(err.Error())
	}

	final, err := sjson.Delete(noVersion, "id")
	if err != nil {
		log.Fatal(err.Error())
	}

	f, err := os.Create(basePath + fmt.Sprint(depth) + "/" + name + "/schema.json")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer f.Close()

	beatiful := bytes.NewBuffer([]byte{})

	err = json.Indent(beatiful, []byte(final), "", "\t")
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = io.WriteString(f, beatiful.String())
	if err != nil {
		log.Fatal(err.Error())
	}
}

func readEnvFile(fileName string) []string {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed opening %s: %s", fileName, err)
	}
	defer f.Close()

	raw, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("failed reading %s: %s", fileName, err)
	}
	return strings.Split(string(raw), "\n")
}
