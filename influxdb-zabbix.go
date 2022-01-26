package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	//"bytes"
	//regexp"
	//"context"
	"github.com/influxdata/influxdb-client-go/v2"


	cfg "github.com/PedroVelezSilva/influxdb-zabbix/config"
	helpers "github.com/PedroVelezSilva/influxdb-zabbix/helpers"
	input "github.com/PedroVelezSilva/influxdb-zabbix/input"
	log "github.com/PedroVelezSilva/influxdb-zabbix/log"
//	influx "github.com/PedroVelezSilva/influxdb-zabbix/output/influxdb"
	registry "github.com/PedroVelezSilva/influxdb-zabbix/reg"
)

var m runtime.MemStats

var exitChan = make(chan int)

var wg sync.WaitGroup

type TOMLConfig cfg.TOMLConfig

var config cfg.TOMLConfig

type DynMap map[string]interface{}

type Param struct {
	input  Input
	output Output
}

type Input struct {
	provider      string
	address       string
	tablename     string
	interval      int
	hoursperbatch int
}

type Output struct {
	address            string
	database           string
	username           string
	password           string
	precision          string
	outputrowsperbatch int
}

type InfluxDB struct {
	output Output
}

var mapTables = make(registry.MapTable)

//
// Gather data
//
func (p *Param) gatherData() error {

	var infoLogs []string
	var currTable string = p.input.tablename
	var currTableForLog string = helpers.RightPad(currTable, " ", 12-len(currTable))

	// read registry
	if err := registry.Read(&config, &mapTables); err != nil {
		fmt.Println(err)
		return err
	}

	// set times
	starttimereg := registry.GetValueFromKey(mapTables, currTable)
	startimerfc, err := time.Parse("2006-01-02T15:04:05", starttimereg)
	if err != nil {
		startimerfc, err = time.Parse(time.RFC3339, starttimereg)
		if err != nil {
			return err
		}
	}

	var starttimestr string = strconv.FormatInt(startimerfc.Unix(), 10)
	var endtimetmp time.Time = startimerfc.Add(time.Hour * time.Duration(p.input.hoursperbatch))
	var endtimestr string = strconv.FormatInt(endtimetmp.Unix(), 10)

	//
	// <--  Extract
	//
	var tlen int = len(currTable)
	infoLogs = append(infoLogs,
		fmt.Sprintf(
			"----------- | %s | [%v --> %v[",
			currTableForLog,
			startimerfc.Format("2006-01-02 15:04:00"),
			endtimetmp.Format("2006-01-02 15:04:00")))

	//start watcher
	startwatch := time.Now()
	ext := input.NewExtracter(
		p.input.provider,
		p.input.address,
		currTable,
		starttimestr,
		endtimestr)

	if err := ext.Extract(); err != nil {
		log.Error(1, "Error while executing script: %s", err)
		return err
	}

	// count rows
	var rowcount int = len(ext.Result)
	infoLogs = append(infoLogs,
		fmt.Sprintf(
			"<-- Extract | %s | %v rows in %s",
			currTableForLog,
			rowcount,
			time.Since(startwatch)))
			
    // set max clock time
	var maxclock time.Time = startimerfc
	if ext.Maxclock.IsZero() == false {
		maxclock = ext.Maxclock
	}

	// no row
	if rowcount == 0 {
		infoLogs = append(infoLogs,
			fmt.Sprintf(
				"--> Load    | %s | No data",
				currTableForLog))
	} else {
		//
		// --> Load
		//
		startwatch = time.Now()
		inlineData := ""

		if rowcount <= p.output.outputrowsperbatch {

			inlineData = strings.Join(ext.Result[:], "\n")

				client := influxdb2.NewClient("url", "<token>")
				// always close client at the end
							
				// get non-blocking write client
				writeAPI := client.WriteAPI("<username>", "<bucket>")

				//SPLIT EACH LINE IN inlineData
				lineData := strings.Split(inlineData, "\n")	
				//FOR EACH LINE OF inlineData 
				for i := 0; i < len(lineData); i++ {
				
				//REMOVE ALL back slashes
					clearBackSlash := strings.ReplaceAll(lineData[i], "\\", "")
					lineData[i] = clearBackSlash
				//	fmt.Println("DEBUG LINE:\n", i, lineData[i] )
				//SPLIT EACH FIELD FOR PARSING on , 
					lineFields := strings.Split(lineData[i], ",")
				//	fmt.Println("DEBUGGING FIELDS from Line ", i);
				//FOR EACH FIELD SWAP whitespaces with _
					newLine :=""
					for l:=0; l < len(lineFields); l++{
						if(l<3){
						    lineFields[l]= strings.ReplaceAll(lineFields[l], " ", "_")
				//			fmt.Println("Field\n",l, lineFields[l]);
							newLine += lineFields[l] + ","
						}else{
					// LAST FIELD is COMBO application + value + timestamp
					// SPLITING string on"value" keyword
							if len(lineFields) > 4{
								fmt.Println("\nline with more than 1 key value pair\n")
								fmt.Println("lineFields ", l, lineFields[l])
								//IF more than 4 value key pairs position 3 is populated with "applications=XXXXX value_min=XXXXXX" so split and clear spaces in applications
								if l==3 {
									appandval := strings.Split(lineFields[l], "value")
									appandval[0] = strings.ReplaceAll(appandval[0], " ", "")
									appandval[1] = "value"+appandval[1]
									reconcat := appandval[0] + " " + appandval[1]
									lineFields[l] =  reconcat
								}
								if l==4 {
									newLine+=lineFields[l]+","
								}else if l<len(lineFields)-1{
									newLine+=lineFields[l]+","
								}else{
									lineFields[l] = strings.ReplaceAll(lineFields[l], " ", "")
									newLine+=lineFields[l]
								}
								fmt.Println("newLine with more than 1 key value pair: ", newLine)	
							}else{
								applicationValueTimeStamp := strings.Split(lineFields[l], "value")
								//fmt.Println("DEBUG applicationValueTimeStamp:\n")
								//CLEANING WHITESPACES from "applications" value
								applicationValueTimeStamp[0] = strings.ReplaceAll(applicationValueTimeStamp[0]," ", "")
								//Re-adding value key
								applicationValueTimeStamp[1] = "value" + applicationValueTimeStamp[1]	
								newLine += applicationValueTimeStamp[0] + " "
								newLine += applicationValueTimeStamp[1]
							}
						}
					} 
					fmt.Println("FINAL CLEAN MEASUREMENT: \n", newLine)
					writeAPI.WriteRecord(fmt.Sprintf(newLine))
					// Flush writes
					writeAPI.Flush()
				}
				
				defer client.Close()

			//if err := loa.Load(); err != nil {
			if err != nil {
				log.Error(1, "Error while loading data for %s. %s", currTable, err)
				return err
			}

			infoLogs = append(infoLogs,
				fmt.Sprintf(
					"--> Load    | %s | %v rows in %s",
					currTableForLog,
					rowcount,
					time.Since(startwatch)))

		} else { // else split result in multiple batches

			var batches float64 = float64(rowcount) / float64(p.output.outputrowsperbatch)
			var batchesCeiled float64 = math.Ceil(batches)
			var batchLoops int = 1
			var minRange int = 0
			var maxRange int = 0

			for batches > 0 { // while
				if batchLoops == 1 {
					minRange = 0
				} else {
					minRange = maxRange
				}

				maxRange = batchLoops * p.output.outputrowsperbatch
				if maxRange >= rowcount {
					maxRange = rowcount
				}

				// create slide
				datapart := []string{}
				for i := minRange; i < maxRange; i++ {
					datapart = append(datapart, ext.Result[i])
				}

				inlineData = strings.Join(datapart[:], "\n")


				startwatch = time.Now()
						
				client := influxdb2.NewClient("<url>", "<token>")
				// always close client at the end
				
				
				// get non-blocking write client
				writeAPI := client.WriteAPI("<username>", "<bucket>")

				

				//SPLIT EACH LINE IN inlineData
				lineData := strings.Split(inlineData, "\n")	
				//FOR EACH LINE OF inlineData 
				for i := 0; i < len(lineData); i++ {
				//REMOVE ALL back slashes
					clearBackSlash := strings.ReplaceAll(lineData[i], "\\", "")
					lineData[i] = clearBackSlash
				//	fmt.Println("DEBUG LINE:\n", i, lineData[i] )
				//SPLIT EACH FIELD FOR PARSING on , 
					lineFields := strings.Split(lineData[i], ",")
				//	fmt.Println("DEBUGGING FIELDS from Line ", i);
				//FOR EACH FIELD SWAP whitespaces with _
					newLine :=""
					for l:=0; l < len(lineFields); l++{
						if(l<3){
						    lineFields[l]= strings.ReplaceAll(lineFields[l], " ", "_")
				//			fmt.Println("Field\n",l, lineFields[l]);
							newLine += lineFields[l] + ","
						}else{
					// LAST FIELD is COMBO application + value + timestamp
					// SPLITING string on"value" keyword
							if len(lineFields) > 4{
								fmt.Println("\nline with more than 1 key value pair\n")
								fmt.Println("lineFields ", l, lineFields[l])
								//IF more than 4 value key pairs position 3 is populated with "applications=XXXXX value_min=XXXXXX" so split and clear spaces in applications
								if l==3 {	
									appandval := strings.Split(lineFields[l], "value")
									appandval[0] = strings.ReplaceAll(appandval[0], " ", "")
									appandval[1] = "value"+appandval[1]
									reconcat := appandval[0] + " " + appandval[1]
									lineFields[l] =  reconcat
								}

								if l==4 {
									newLine+=lineFields[l]+","
								}else if l<len(lineFields)-1{
									newLine+=lineFields[l]+","
								}else{
									newLine+=lineFields[l]
								}
								fmt.Println("newLine with more than 1 key value pair: ", newLine)
							}else{
								applicationValueTimeStamp := strings.Split(lineFields[l], "value")
								//fmt.Println("DEBUG applicationValueTimeStamp:\n")
								//CLEANING WHITESPACES from "applications" value
								applicationValueTimeStamp[0] = strings.ReplaceAll(applicationValueTimeStamp[0]," ", "")
								//Re-adding value key
								applicationValueTimeStamp[1] = "value" + applicationValueTimeStamp[1]
			
								newLine += applicationValueTimeStamp[0] + " "
								newLine += applicationValueTimeStamp[1]
							}
						}
					} 
					fmt.Println("FINAL CLEAN MEASUREMENT: \n", newLine)
					writeAPI.WriteRecord(fmt.Sprintf(newLine))
					// Flush writes
					writeAPI.Flush()
				}
				defer client.Close()
				
				//if err := loa.Load(); err != nil {
				if err != nil {
					log.Error(1, "Error while loading data for %s. %s", currTable, err)
					return err
				}
				// log
				tableBatchName := fmt.Sprintf("%s (%v/%v)",
					currTable,
					batchLoops,
					batchesCeiled)
				tlen = len(tableBatchName)
				infoLogs = append(infoLogs,
					fmt.Sprintf("--> Load    | %s | %v rows in %s",
						helpers.RightPad(tableBatchName, " ", 13-tlen),
						len(datapart),
						time.Since(startwatch)))
				batchLoops += 1
				batches -= 1
			} // end while
		}
	}
	// Save in registry
	saveMaxTime(currTable, startimerfc, maxclock, p.input.hoursperbatch)
	tlen = len(currTable)
	infoLogs = append(infoLogs,
		fmt.Sprintf("--- Waiting | %s | %v sec ",
			currTableForLog,
			p.input.interval))

	if config.Logging.LevelFile == "Trace" || config.Logging.LevelConsole == "Trace" {
		runtime.ReadMemStats(&m)
		log.Trace(fmt.Sprintf("--- Memory usage: Alloc = %s | TotalAlloc = %s | Sys = %s | NumGC = %v", 
			helpers.IBytes(m.Alloc / 1024), 
			helpers.IBytes(m.TotalAlloc / 1024), 
			helpers.IBytes(m.Sys / 1024), 
			m.NumGC))
	}

				
	// print all log messages
	print(infoLogs)

	return nil
}

//
// Print all messages
//
func print(infoLogs []string) {
	for i := 0; i < len(infoLogs); i++ {
		log.Info(infoLogs[i])
	}
}

//
// Save max time 
//
func saveMaxTime(tablename string, starttime time.Time, maxtime time.Time, duration int) {

	var timetosave time.Time

	// if maxtime is greater than now, keep the maxclock returned 
	if (starttime.Add(time.Hour * time.Duration(duration))).After(time.Now()) {
		timetosave = maxtime
	} else {
		timetosave = starttime.Add(time.Hour * time.Duration(duration))
	}

	registry.Save(config,
		tablename,
		timetosave.Format(time.RFC3339))
}

//
// Gather data loop
//
func (p *Param) gather() error {
	for {
		err := p.gatherData()
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(p.input.interval) * time.Second)
	}
	return nil
}

//
// Init
//
func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

//
//  Read TOML configuration
//
func readConfig() {

	// command-line flag parsing
	flag.Parse()

	// read configuration file
	if err := cfg.Parse(&config); err != nil {
		fmt.Println(err)
		return
	}
	// validate configuration file
	if err := cfg.Validate(&config); err != nil {
		fmt.Println(err)
		return
	}
}

//
// Read registry file
//
func readRegistry() {
	if err := registry.Read(&config, &mapTables); err != nil {
		log.Error(0, err.Error())
		return
	}
}

//
// Init global logging
//
func initLog() {
	log.Init(config)
}

//
// Listen to System Signals
//
func listenToSystemSignals() {
	signalChan := make(chan os.Signal, 1)
	code := 0

	signal.Notify(signalChan, os.Interrupt)
	signal.Notify(signalChan, os.Kill)
	signal.Notify(signalChan, syscall.SIGTERM)

	select {
	case sig := <-signalChan:
		log.Info("Received signal %s. shutting down", sig)
	case code = <-exitChan:
		switch code {
		case 0:
			log.Info("Shutting down")
		default:
			log.Warn("Shutting down")
		}
	}
	log.Close()
	os.Exit(code)
}

//
// Main
//
func main() {

	log.Info("***** Starting influxdb-zabbix *****")

	// listen to System Signals
	go listenToSystemSignals()

	readConfig()
	readRegistry()
	initLog()

	// set of active tables
	log.Trace("--- Active tables:")
	var tables = []*cfg.Table{}
	for _, table := range config.Tables {
		if table.Active {
			var tlen int = len(table.Name)
			var durationh string
			var duration time.Duration = time.Duration(table.Hoursperbatch) * time.Hour
			if (duration.Hours() >= 24) {
				durationh = fmt.Sprintf("%v days per batch", duration.Hours()/24)
			} else {
				durationh = fmt.Sprintf("%v hours per batch", duration.Hours())
			}

			log.Trace(
				fmt.Sprintf(
					"----------- | %s | Each %v sec | %s | Output by %v",
					helpers.RightPad(table.Name, " ", 12-tlen),
					table.Interval,
					durationh,
					table.Outputrowsperbatch))

			tables = append(tables, table)
		}
	}

	log.Info("--- Start polling")

	var provider string = (reflect.ValueOf(config.Zabbix).MapKeys())[0].String()
	var address string = config.Zabbix[provider].Address
	log.Trace(fmt.Sprintf("--- Provider: %s", provider))

	influxdb := config.InfluxDB
	
	for _, table := range tables {

		input := Input{
			provider,
			address,
			table.Name,
			table.Interval,
			table.Hoursperbatch}

		output := Output{
			influxdb.Url,
			influxdb.Database,
			influxdb.Username,
			influxdb.Password,
			influxdb.Precision,
			table.Outputrowsperbatch}

		p := &Param{input, output}

		wg.Add(1)
		go p.gather()
	}
	wg.Wait()
}
