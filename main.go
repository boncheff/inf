package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/han/influxdb/client/v2"
	influx "github.com/han/influxdb/client/v2"
)

// queryDB convenience function to query the database
func queryDB(c influx.Client, db string, dryRun bool, numberOfChunks uint64) {
	var metrics = []string{"network-in-percent", "network-out-percent"}

	for _, metric := range metrics {

		var minTimestamp uint64 = math.MaxUint64
		qq := influx.NewQuery(fmt.Sprintf("SELECT first(value) FROM /%s/;", metric), db, "ns")
		if r, err := c.Query(qq); err == nil {
			if r.Error() != nil {
				log.Infof("Response error querying 'first' value: %+v", r.Error())
			}

			for _, res := range r.Results {
				for _, s := range res.Series {
					for _, v := range s.Values {
						ts, err := strconv.ParseUint(string(v[0].(json.Number)), 10, 64)
						if err != nil {
							log.Infof("Failed to parse timestamps")
							break
						}
						if ts < minTimestamp {
							minTimestamp = ts
						}
					}
				}
			}
		}
		ts, _ := time.Parse("2006-01-02 15:04", "2017-07-04 14:00")
		maxDate := uint64(ts.UnixNano())
		timePeriod := (maxDate - minTimestamp) / numberOfChunks

		if minTimestamp < maxDate {
			var i uint64
			for i = 0; i < numberOfChunks; i++ {
				from := minTimestamp + i*timePeriod
				to := minTimestamp + (i+1)*timePeriod

				q := influx.NewQuery(fmt.Sprintf("SELECT * FROM /%s/ WHERE time >= %d and time < %d", metric, from, to), db, "ns")
				if response, err := c.Query(q); err == nil {
					if response.Error() != nil {
						log.Infof("Query response error: %+v", response.Error())
					}

					count := 0
					fmt.Println("")

					for _, r := range response.Results {
						for _, s := range r.Series {

							tags := map[string]string{}
							fields := map[string]interface{}{}

							bp, err := influx.NewBatchPoints(client.BatchPointsConfig{
								Database:  db,
								Precision: "ns",
							})
							if err != nil {
								log.Fatal(err)
							}

							for _, vals := range s.Values {

								var (
									value float64
									ts    int64
								)

								for i, val := range vals {

									if s.Columns[i] == "value" {
										if v, ok := val.(json.Number); ok {
											value, err = strconv.ParseFloat(string(v), 64)
											if err != nil {
												log.Errorf("Failed to parse metric value of type json.Number %v for metric %s", val, s.Name)
												break
											}

										} else {
											log.Errorf("Invalid metric value type %s for metric %s", v, s.Name)
											break
										}

									} else if s.Columns[i] == "time" {
										ts, err = strconv.ParseInt(string(val.(json.Number)), 10, 64)
										if err != nil {
											log.Infof("Failed to parse timestamps")
											break
										}

									} else {
										if tag, ok := val.(string); ok {
											tags[s.Columns[i]] = tag
										} else {
											tags[s.Columns[i]] = ""
										}
									}

								}
								if value > 0 {
									count++

									value *= 8
									fields["value"] = value

									if dryRun {
										if count%100 == 0 {
											fmt.Print(".")
										}
									}

									pt, err := influx.NewPoint(
										s.Name,
										tags,
										fields,
										time.Unix(0, ts),
									)
									if err != nil {
										log.Fatal(err)
									}

									bp.AddPoint(pt)

								}
							}
							if !dryRun {
								if err := c.Write(bp); err != nil {
									log.Fatal(err)
								}
							}
						}
					}
				}
			}
		}
	}
}

func main() {
	dryRun := flag.Bool("dryrun", false, "Dry Run")
	influxIP := flag.String("influxip", "172.17.42.1", "Influx IP")
	numberOfChunks := flag.Int("chunks", 100, "Number of chunks to get from InfluxDB")
	flag.Parse()

	influxConfig := influx.HTTPConfig{
		Addr:               fmt.Sprintf("http://%s", *influxIP),
		InsecureSkipVerify: true,
	}

	influxClient, err := influx.NewHTTPClient(influxConfig)
	if err != nil {
		log.Fatalf("Failed to create InfluxDB client: %s", err.Error())
	}
	defer influxClient.Close()

	customerID := "dbu" + strings.Replace("416c300b-2164-4ecd-92e0-ff77fcf08bec", "-", "_", -1)
	queryDB(influxClient, customerID, *dryRun, uint64(*numberOfChunks))
}
