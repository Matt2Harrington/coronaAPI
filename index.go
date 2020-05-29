package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/alecthomas/gometalinter/_linters/src/gopkg.in/yaml.v2"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	_ "github.com/lib/pq"
)

var (
	pg Postgres
	db *sql.DB
	err error
)

// Coronavirus struct
type Data struct {
	ID 					uuid.UUID
	Country             string
	Cases               int
	CasesToday          int
	Deaths              int
	DeathsToday         int
	Recovered           int
	Active              int
	Critical            int
	CasesPerOneMillion  float32
	DeathsPerOneMillion float32
	Updated             time.Time
	TimeRan  	  		time.Time
	//Info 				Info

}

// CountryInfo struct
type Info struct {
	ID 		  	  uuid.UUID
	APIID        int
	Latitude  	  float64
	Longitude 	  float64
	DataID    	  uuid.UUID
	Updated  	  time.Time
	TimeRan  	  time.Time
}

type dataInfo struct {
	DataList []Data
	//InfoList []Info
}

// Postgres info
type Postgres struct {
	Host string  `yaml:"host"`
	Port int     `yaml:"port"`
	User string  `yaml:"username"`
	DBName string`yaml:"databaseName"`
}

var coronaData []Data

func (c *Postgres) getPostgres() *Postgres {
	yamlFile, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return c
}

func (c *Postgres) getPostgresENV() []string {
	var values []string
	hostEnv := os.Getenv("host")
	usernameEnv := os.Getenv("username")
	passwordEnv := os.Getenv("password")
	portEnv := os.Getenv("port")
	dataNameEnv := os.Getenv("database")
	
	values = append(values, hostEnv)
	values = append(values, portEnv)
	values = append(values, usernameEnv)
	values = append(values, passwordEnv)
	values = append(values, dataNameEnv)

	return values
}

func setUpPostgres() (*sql.DB, error) {
	values := pg.getPostgresENV()
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		values[0], values[1], values[2], values[3], values[4])

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	fmt.Println("Successfully connected!")
	return db, nil
}

func homePage(w http.ResponseWriter, r *http.Request){
	fmt.Fprintf(w, "Welcome to the HomePage!")
	fmt.Println("Endpoint Hit: homePage")
}

func handleRequests() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/corona", indexHandler)
	http.HandleFunc("/new", indexDistinctNewestEntryHandler)
	//http.HandleFunc("/corona/{country}", returnSingleCountry)
	log.Fatal(http.ListenAndServe(":10000", nil))
}

// parseParams accepts a req and returns the `num` path tokens found after the `prefix`.
// returns an error if the number of tokens are less or more than expected
func parseParams(req *http.Request, prefix string, num int) ([]string, error) {
	url := strings.TrimPrefix(req.URL.Path, prefix)
	params := strings.Split(url, "/")
	if len(params) != num || len(params[0]) == 0 || len(params[1]) == 0 {
		return nil, fmt.Errorf("Bad format. Expecting exactly %d params", num)
	}
	return params, nil
}

// indexHandler calls `queryRepos()` and marshals the result as JSON
func indexHandler(w http.ResponseWriter, req *http.Request) {
	data := dataInfo{}

	err = queryData(&data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	out, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Println("Endpoint Hit: Data")
	fmt.Fprintf(w, string(out))
}

// indexHandler calls `queryRepos()` and marshals the result as JSON
func indexDistinctNewestEntryHandler(w http.ResponseWriter, req *http.Request) {
	data := dataInfo{}

	err = queryDistinctNewestEntries(&data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	out, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Println("Endpoint Hit: Newest Entry Data")
	fmt.Fprintf(w, string(out))
}

//func returnSingleCountry(w http.ResponseWriter, r *http.Request) {
//	vars := mux.Vars(r)
//	key := vars["country"]
//
//	for _, data := range coronaData {
//		if data.Country == key {
//			json.NewEncoder(w).Encode(data)
//		}
//	}
//}

// queryDistinctNewest
func queryDistinctNewestEntries(dataInfo *dataInfo) error {
	rows, err := db.Query(`
		select distinct on (country)
		updated, country, cases, cases_today, deaths, deaths_today, recovered, active, 
		critical, cases_per_one_million, deaths_per_one_million
		FROM data ORDER BY country, updated DESC;`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		data := Data{}
		//info := Info{}
		err = rows.Scan(
			&data.Updated,
			&data.Country,
			&data.Cases,
			&data.CasesToday,
			&data.Deaths,
			&data.DeathsToday,
			&data.Recovered,
			&data.Active,
			&data.Critical,
			&data.CasesPerOneMillion,
			&data.DeathsPerOneMillion,
		)
		if err != nil {
			return err
		}
		dataInfo.DataList = append(dataInfo.DataList, data)
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}

// queryRepos first fetches the repositories data from the db
func queryData(dataInfo *dataInfo) error {
	rows, err := db.Query(`
		SELECT * FROM data ORDER BY cases ASC `)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		data := Data{}
		//info := Info{}
		err = rows.Scan(
			&data.ID,
			&data.Country,
			&data.Cases,
			&data.CasesToday,
			&data.Deaths,
			&data.DeathsToday,
			&data.Recovered,
			&data.Active,
			&data.Critical,
			&data.CasesPerOneMillion,
			&data.DeathsPerOneMillion,
			&data.Updated,
			&data.TimeRan,
			//&info.ID,
			//&info.APIID,
			//&info.Latitude,
			//&info.Longitude,
			//&info.DataID,
			//&info.Updated,
			//&info.TimeRan,
		)
		if err != nil {
			return err
		}
		dataInfo.DataList = append(dataInfo.DataList, data)
		//dataInfo.InfoList = append(dataInfo.InfoList, info)
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	// call to database to setup
	db, err = setUpPostgres()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	handleRequests()
}

