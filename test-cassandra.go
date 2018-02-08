package main

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	// Create a Cassandra cluster
	cluster := gocql.NewCluster("127.0.0.1")

	// Settings for createing the new Session
	cluster.Keyspace = "weaviate"
	cluster.ConnectTimeout = time.Minute
	cluster.Timeout = time.Hour
	session, err := cluster.CreateSession()

	if err != nil {
		panic(err)
	}

	testCassandraMapGo(session)
	testCassandraWideGo(session)
	testCassandraWideQuery(session)
}

func testCassandraMapGo(session *gocql.Session) {
	defer timeTrack(time.Now(), "Loop through map testresults")

	iter := session.Query(`
		SELECT * FROM weaviate.object_data_map where uuid = 5a1226bb-cf92-4f46-95f7-571c5b0988b2;
	`).Iter()

	maxTimeStamp := int64(0)
	// maxValue := ""
	maxTimeStamp2 := int64(0)
	// maxValue2 := ""
	for {
		m := map[string]interface{}{}
		if !iter.MapScan(m) {
			break
		}

		for k, _ := range m["testint"].(map[time.Time]string) {
			if k.Unix() > maxTimeStamp {
				maxTimeStamp = k.Unix()
				// maxValue = v
			}
		}

		for k, _ := range m["teststring"].(map[time.Time]string) {
			if k.Unix() > maxTimeStamp2 {
				maxTimeStamp2 = k.Unix()
				// maxValue2 = v
			}
		}
	}

	// fmt.Println(fmt.Sprintf("DEBUG: testint: Time: %d, Value: %s", maxTimeStamp, maxValue))
	// fmt.Println(fmt.Sprintf("DEBUG: teststring: Time: %d, Value: %s", maxTimeStamp2, maxValue2))
}

func testCassandraWideGo(session *gocql.Session) {
	defer timeTrack(time.Now(), "Loop through wide testresults")

	iter := session.Query(`
		SELECT * FROM weaviate.object_data_wide where uuid = f87341ec-091a-4c15-b622-276380c617f9;
	`).Iter()

	maxTimeStamp := int64(0)
	// maxValue := ""
	maxTimeStamp2 := int64(0)
	// maxValue2 := ""
	for {
		m := map[string]interface{}{}
		if !iter.MapScan(m) {
			break
		}

		k := m["timestamp"].(time.Time)
		// v := m["testint"].(string)
		if k.Unix() > maxTimeStamp {
			maxTimeStamp = k.Unix()
			// maxValue = v
		}

		k2 := m["timestamp"].(time.Time)
		// v2 := m["teststring"].(string)
		if k2.Unix() > maxTimeStamp2 {
			maxTimeStamp2 = k2.Unix()
			// maxValue2 = v2
		}
	}

	// fmt.Println(fmt.Sprintf("DEBUG: testint: Time: %d, Value: %s", maxTimeStamp, maxValue))
	// fmt.Println(fmt.Sprintf("DEBUG: teststring: Time: %d, Value: %s", maxTimeStamp2, maxValue2))
}

func testCassandraWideQuery(session *gocql.Session) {
	defer timeTrack(time.Now(), "Do a direct query with limit")

	// Should be....? Doesnt work for a reason..?
	// SELECT * FROM weaviate.object_data_wide where uuid = f87341ec-091a-4c15-b622-276380c617f9 where timestamp < 50000000 LIMIT 1;
	iter := session.Query(`
		SELECT * FROM weaviate.object_data_wide where uuid = f87341ec-091a-4c15-b622-276380c617f9 LIMIT 1;
		`).Iter()

	maxTimeStamp := int64(0)
	maxValue := ""
	maxTimeStamp2 := int64(0)
	maxValue2 := ""
	for {
		m := map[string]interface{}{}
		if !iter.MapScan(m) {
			break
		}

		k := m["timestamp"].(time.Time)
		v := m["testint"].(string)
		if k.Unix() > maxTimeStamp {
			maxTimeStamp = k.Unix()
			maxValue = v
		}

		k2 := m["timestamp"].(time.Time)
		v2 := m["teststring"].(string)
		if k2.Unix() > maxTimeStamp2 {
			maxTimeStamp2 = k2.Unix()
			maxValue2 = v2
		}
	}

	fmt.Println(fmt.Sprintf("DEBUG: testint: Time: %d, Value: %s", maxTimeStamp, maxValue))
	fmt.Println(fmt.Sprintf("DEBUG: teststring: Time: %d, Value: %s", maxTimeStamp2, maxValue2))

}

func timeTrack(start time.Time, info ...string) {
	elapsed := time.Since(start)

	// Skip this function, and fetch the PC and file for its parent
	pc, _, line, _ := runtime.Caller(1)

	// Retrieve a Function object this functions parent
	functionObject := runtime.FuncForPC(pc)

	// Regex to extract just the function name (and not the module path)
	extractFnName := regexp.MustCompile(`^.*\/(.*)$`)
	name := extractFnName.ReplaceAllString(functionObject.Name(), "$1")

	infoStr := ""
	if len(info) > 0 {
		infoStr = strings.Join(info, ", ") + ": "
	}

	fmt.Println(fmt.Sprintf("%s%s:%d took %s", infoStr, name, line, elapsed))
}
