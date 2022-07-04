package main

import (
	"github.com/hpcloud/tail"
	"strconv"
	"encoding/json"
    "fmt"
    "io/ioutil"
    "os"  
	"gopkg.in/yaml.v2"
    "net/http"
    "strings"
    "time"
    "regexp"
)


type postData struct {
	ConfigId            string       `json:"configId"`
	ExecuteTime         int64        `json:"executeTime"`
	Sql                 string       `json:"sql"`
}

func isValueInList(value string, list []string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func delItem(vs []string, s string) []string{
	for i := 0; i < len(vs); i++ {
		if s == vs[i] {
			vs = append(vs[:i], vs[i+1:]...)
		}
	}
	return vs
}

func slowCheck(UrlDBSlow ,mongoLogPath ,backupOperateIpList ,dbNameList ,configId string ,slowOpStd int64) {
	var keyIDList ,keyID ,removeKeyID ,executeTime 	[]string
	var sqlStr string
	seek := &tail.SeekInfo{}
	seek.Offset = 0
	seek.Whence = os.SEEK_END
	config := tail.Config{}
	config.Follow = true
	config.Location = seek
	f, err := tail.TailFile(mongoLogPath, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if info := recover(); info != nil {
			fmt.Println("触发了宕机", info)
		} else {
			fmt.Println("程序正常退出")
		}
	}()

	Loop:
	_, err = os.Stat(mongoLogPath)
	if err != nil {
		return
	}
	for sql := range f.Lines {
		sqlStr = sql.Text
		lineTail := strings.Split(sqlStr ," ")[len(strings.Split(sqlStr ," "))-1]
		if strings.Contains(sqlStr ," NETWORK ") && strings.Contains(sqlStr ,"accepted") {
			for _, backupOperateIp := range strings.Split(backupOperateIpList ,",") {
				if strings.Contains(sqlStr ,backupOperateIp) {
					headkeyID := regexp.MustCompile(`#\d+`)
					keyID = headkeyID.FindStringSubmatch(sqlStr)
					keyidNum := regexp.MustCompile(`\d+`)
					keyIDNumStr := keyidNum.FindStringSubmatch(keyID[0])
					keyIDList = append(keyIDList ,keyIDNumStr[0])
					fmt.Println("add keyIDList: ")
					fmt.Println(keyIDList)
				} else {
					goto Loop
				}
			}
			goto Loop
		} else if strings.Contains(lineTail ,"ms") && strings.Contains(sqlStr ,"conn") {
/*					lineTail := strings.Split(sqlStr ,"op_query")[len(strings.Split(sqlStr ,"op_query"))-1]
*/				
				executeTimeMSRegexp := regexp.MustCompile(`\d+ms`)
				executeTimeMS := executeTimeMSRegexp.FindStringSubmatch(lineTail)
				executeTimeRegexp := regexp.MustCompile(`\d+`)
				if len(executeTimeMS) != 0 {
					executeTime = executeTimeRegexp.FindStringSubmatch(executeTimeMS[0])
				} else {
					goto Loop
				}
				connsqlKeyIDRegexp := regexp.MustCompile(`conn\d+`)
				connsqlKeyID := connsqlKeyIDRegexp.FindStringSubmatch(sqlStr)
				sqlKeyIDRegexp := regexp.MustCompile(`\d+`)
				if len(connsqlKeyID) != 0 {
					sqlKeyID := sqlKeyIDRegexp.FindStringSubmatch(connsqlKeyID[0])
					if len(sqlKeyID) != 0 {
						if isValueInList(sqlKeyID[0] ,keyIDList) {
							goto Loop
						}
					}
				} else {
					goto Loop
				}
				executeTimeInt ,_ := strconv.ParseInt(executeTime[0] ,10 ,64)
				count := 0
				for _, dbNameStr := range strings.Split(dbNameList, ",") {
					configIdStr := strings.Split(configId ,",")[count]
					count++
					if len(executeTime) != 0 && strings.Contains(sqlStr, dbNameStr) && executeTimeInt > slowOpStd { 
						data := postData{ConfigId: configIdStr, ExecuteTime: executeTimeInt, Sql: sqlStr}
						dataJson, _ := json.Marshal(data)
						dataJsonStr := string(dataJson)
						fmt.Println(dataJsonStr)
						Post(UrlDBSlow ,dataJsonStr)
					}
				}
				
		} else if strings.Contains(sqlStr ," NETWORK ") && strings.Contains(sqlStr ,"end") {
			connRemoveKeyIDRegexp := regexp.MustCompile(`conn\d+`)
			connRemoveKeyID := connRemoveKeyIDRegexp.FindStringSubmatch(sqlStr)
			removeKeyIDRegexp := regexp.MustCompile(`\d+`)
			removeKeyID = removeKeyIDRegexp.FindStringSubmatch(connRemoveKeyID[0])
			if isValueInList(removeKeyID[0] ,keyIDList) {
				fmt.Println("删除前的keyIDList: ")
				fmt.Println(keyIDList)
				keyIDList = delItem(keyIDList ,removeKeyID[0])
				fmt.Println("删除后的keyIDList: ")
				fmt.Println(keyIDList)
			}
		}
	}
}

func heartBeat(urlDBSlow ,configId ,mongoLogPath string) {
	var sqlStr string
	var executeTimeInt int64 = 0
	for {
		f ,err := os.Open(mongoLogPath)
		if err != nil {
			continue
		}
		time.Sleep(900 * time.Second)
		fmt.Println(time.Now().String())
		for _, configIdStr := range strings.Split(configId, ",") {
			data := postData{ConfigId: string(configIdStr), ExecuteTime: executeTimeInt, Sql: sqlStr}
			dataJson, _ := json.Marshal(data)
			dataJsonStr := string(dataJson)
			fmt.Println(dataJsonStr)
			Post(urlDBSlow ,dataJsonStr)
		}
		defer func() {
			if info := recover(); info != nil {
				fmt.Println("触发了宕机", info)
			} else {
				fmt.Println("程序正常退出")
			}
		}()
		defer f.Close()
	}
}

type conf struct {
	UrlDBSlow           string `yaml:"urlDBSlow"`
	ConfigId            string `yaml:"configId"`
	MongoLogPath        string `yaml:"mongoLogPath"`
	BackupOperateIp     string `yaml:"backupOperateIp"`
	DbNameList          string `yaml:"dbNameList"`
	SlowOpStd           int64 `yaml:"slowOpStd"`
}


func (c *conf) getYaml() *conf {
	yamlFile ,err := ioutil.ReadFile("dbSlowOp.yaml")
	if err != nil {
		fmt.Println("yamlFile.Get err", err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		fmt.Println("Unmarshal: ", err.Error())
	}
	return c
}

func fileMonitoring(UrlDBSlow ,mongoLogPath ,backupOperateIpList ,dbNameList ,configId string , slowOpStd int64, f func(string ,string ,string ,string ,string ,int64)) {
	for {
		f(UrlDBSlow ,mongoLogPath ,backupOperateIpList ,dbNameList ,configId ,slowOpStd)
		time.Sleep(1 * time.Second)
	}
}

func main() {
	var yamlConfig conf
	yamlconf := yamlConfig.getYaml()
	urlDBSlow := yamlconf.UrlDBSlow
	configId := yamlconf.ConfigId
	dbNameList := yamlconf.DbNameList
	mongoLogPath := yamlconf.MongoLogPath
	backupOperateIp := yamlconf.BackupOperateIp
	slowOpStd := yamlconf.SlowOpStd
	fmt.Println("slow log check start!!")
	go fileMonitoring(urlDBSlow ,mongoLogPath ,backupOperateIp ,dbNameList ,configId ,slowOpStd ,slowCheck)
	go heartBeat(urlDBSlow ,configId ,mongoLogPath)
	select{}
}


func Post(url string, data string) (string ,int) {
	jsoninfo := strings.NewReader(data)
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, jsoninfo)
	if err != nil {
	    // handle error
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", "xxx")
	
	resp, err := client.Do(req)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			return
	}
		fmt.Println("Process panic done Post")
	}()
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
	    // handle error
	}
	fmt.Println(string(body))
	fmt.Println(resp.StatusCode)
	return string(body) ,resp.StatusCode
}
