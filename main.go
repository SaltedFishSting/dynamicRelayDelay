package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

//配置文件yaml
type RConfig struct {
	Gw struct {
		Addr           string `yaml:"addr"`
		HttpListenPort int    `yaml:"httpListenPort"`
		DBaddr         string `yaml:"dbaddr"`
		DBname         string `yaml:"dbname"`
		Tablename      string `yaml:"tablename"`
	}
	Output struct {
		Prometheus      bool   `yaml:"prometheus"`
		PushGateway     bool   `yaml:"pushGateway"`
		PushGatewayAddr string `yaml:"pushGatewayAddr"`
		MonitorID       string `yaml:"monitorID"`
		Period          int    `yaml:"period"`
	}
	Relay struct {
		Relaynode          map[int64]string    `yaml:"relaynode"`
		HistogramOptsparam map[string]float64  `yaml:"histogramOptsparam"`
		SummaryOptsparam   map[float64]float64 `yaml:"summaryOptsparam"`
	}
}

var globeCfg *RConfig

//mongodb 数据mode
type BaseLog struct {
	CallBaseLog map[string]string `bson:"callBaseLog"`
	Sid         string            `bson:"sid"`
}

type itime struct {
	InsertTime int64 `bson:"insertTime"`
}

//relay 节点数组 map
var relayidMap = make(map[int64]string)

//HistogramOpt参数
var HistogramOptsparamMap = make(map[string]float64)

//SummaryOpt参数
var SummaryOptsparamMap = make(map[float64]float64)

//上一次的数据库的最后插入时间
var lasttime int64 = 0

//prometheus var
var (
	nodeh *(prometheus.HistogramVec)
	nodes *(prometheus.SummaryVec)
)

//给prometheus推送数据
func extract(CLU map[string]string, CRD map[string]string, CRU map[string]string, CLD map[string]string) error {

	for k, v := range CLU {

		if strings.Index(k, "_") >= 0 {
			ipidlogindex := strings.Split(k, "_")
			if ipidlogindex[1] != "-1" {
				idlogindex := strings.Split(ipidlogindex[0], "|")
				delay, _ := strconv.ParseFloat(v, 64)

				Observe(ipidlogindex[1], idlogindex[1], "Down", delay)
			}
		}
	}
	for k, v := range CRD {

		if strings.Index(k, "_") >= 0 {

			ipidlogindex := strings.Split(k, "_")
			if ipidlogindex[1] != "-1" {
				idlogindex := strings.Split(ipidlogindex[0], "|")
				delay, _ := strconv.ParseFloat(v, 64)

				Observe(ipidlogindex[1], idlogindex[1], "Up", delay)
			}
		}
	}
	for k, v := range CRU {

		if strings.Index(k, "_") >= 0 {
			ipidlogindex := strings.Split(k, "_")
			if ipidlogindex[1] != "-1" {
				idlogindex := strings.Split(ipidlogindex[0], "|")
				delay, _ := strconv.ParseFloat(v, 64)

				Observe(ipidlogindex[1], idlogindex[1], "Down", delay)
			}
		}
	}
	for k, v := range CLD {

		if strings.Index(k, "_") >= 0 {
			ipidlogindex := strings.Split(k, "_")
			if ipidlogindex[1] != "-1" {
				idlogindex := strings.Split(ipidlogindex[0], "|")
				delay, _ := strconv.ParseFloat(v, 64)

				Observe(ipidlogindex[1], idlogindex[1], "Up", delay)
			}
		}
	}
	return nil
}

func Observe(id string, ip string, direction string, delay float64) {
	intid, _ := strconv.ParseInt(id, 10, 64)
	if ip == relayidMap[intid] {
		nodeh.WithLabelValues(id, ip, direction).Observe(delay)
		nodes.WithLabelValues(id, ip, direction).Observe(delay)
	}
}

//从mongedb获取以解码的callBaseLog数据数组
func mongodbToOrtp(ip string, db string, table string) (map[string]string, map[string]string, map[string]string, map[string]string) {

	looptime := int64(globeCfg.Output.Period) //minute
	session, err := mgo.Dial(ip)
	//session, err := mgo.Dial("127.0.0.1:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	collection := session.DB(db).C(table)

	var nowtime itime
	err = collection.Find(bson.M{}).Sort("-insertTime").Limit(1).Select(bson.M{"insertTime": 1}).One(&nowtime)

	var min10time int64
	if lasttime == 0 {
		min10time = nowtime.InsertTime - looptime*1000
	} else {
		min10time = lasttime
	}
	var BaseLogresult []BaseLog

	//通过sid获取callBaseLog日志
	err = collection.Find(bson.M{"insertTime": bson.M{"$gt": min10time, "$lt": nowtime.InsertTime}, "callBaseLog": bson.M{"$exists": true}}).Select(bson.M{"callBaseLog": 1, "sid": 1}).All(&BaseLogresult)
	if err != nil {
		panic(err)
	}
	var ortpCLUString = make(map[string]string)
	var ortpCRDString = make(map[string]string)
	var ortpCRUString = make(map[string]string)
	var ortpCLDString = make(map[string]string)

	for _, result := range BaseLogresult {

		for _, v := range result.CallBaseLog {

			if v != "" && strings.Index(v, "=") >= 0 {
				strrune := []rune(v)
				if string(strrune[10:14]) == "ortp" {

					smap := make(map[string]string)
					strarrays := strings.Split(v, " ")
					for _, v := range strarrays {
						if v != "" && strings.Index(v, "=") >= 0 {

							strarray := strings.Split(v, "=")
							smap[strarray[0]] = strarray[1]
						}
					}
					if d, _ := strconv.ParseFloat(smap["delay_aver"], 64); d > 20000 {

						continue
					}
					smap["sid"] = result.Sid
					switch smap["sub_type"] {
					case "CLU":

						if smap["delay_aver"] != "0" {

							stra := []string{smap["time"], smap["logIndex"], "|", smap["dst"]}
							strkey := strings.Join(stra, "")
							ortpCLUString[strkey] = smap["delay_aver"]
						}
					case "CRD":

						if smap["delay_aver"] != "0" {
							stra := []string{smap["time"], smap["logIndex"], "|", smap["src"]}
							strkey := strings.Join(stra, "")
							ortpCRDString[strkey] = smap["delay_aver"]
						}
					case "CRU":

						if smap["delay_aver"] != "0" {
							stra := []string{smap["time"], smap["logIndex"], "|", smap["dst"]}
							strkey := strings.Join(stra, "")
							ortpCRUString[strkey] = smap["delay_aver"]
						}
					case "CLD":

						if smap["delay_aver"] != "0" {
							stra := []string{smap["time"], smap["logIndex"], "|", smap["src"]}
							strkey := strings.Join(stra, "")
							ortpCLDString[strkey] = smap["delay_aver"]
						}

					}
				}
			}
		}
	}

	return ortpCLUString, ortpCRDString, ortpCRUString, ortpCLDString
}

func loadConfig() {
	cfgbuf, err := ioutil.ReadFile("cfg.yaml")
	if err != nil {
		panic("not found cfg.yaml")
	}
	rfig := RConfig{}
	err = yaml.Unmarshal(cfgbuf, &rfig)
	if err != nil {
		panic("invalid cfg.yaml")
	}
	globeCfg = &rfig
	fmt.Println("Load config -'cfg.yaml'- ok...")
}
func init() {
	loadConfig() //加载配置文件
	relayidMap = globeCfg.Relay.Relaynode
	HistogramOptsparamMap = globeCfg.Relay.HistogramOptsparam
	SummaryOptsparamMap = globeCfg.Relay.SummaryOptsparam

	nodeh = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "H",
		Name:      "relay",
		Help:      "nodeh",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"RelayId",
			"IP",
			"direction",
		})

	nodes = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "S",
		Name:       "relay",
		Help:       "nodes",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"RelayId",
			"IP",
			"direction",
		})

	prometheus.MustRegister(nodeh)
	prometheus.MustRegister(nodes)

}
func main() {

	ip := globeCfg.Gw.DBaddr       //"103.25.23.89:60013"
	db := globeCfg.Gw.DBname       //"dataAnalysis_new"
	table := globeCfg.Gw.Tablename //"report_tab"
	//loop
	go func() {
		fmt.Println("Program startup ok...")
		//获取callBaseLog数据
		for {

			CLU, CRD, CRU, CLD := mongodbToOrtp(ip, db, table)

			extract(CLU, CRD, CRU, CLD)
			//是否推送数据给PushGatway
			if globeCfg.Output.PushGateway {
				var info = make(map[string]string)
				info["monitorID"] = globeCfg.Output.MonitorID
				if err := push.FromGatherer("rt", info, globeCfg.Output.PushGatewayAddr, prometheus.DefaultGatherer); err != nil {
					fmt.Println("FromGatherer:", err)
				}
			}
			fmt.Println(time.Now())
			time.Sleep(time.Duration(globeCfg.Output.Period) * time.Second)

		}
	}()
	//设置prometheus监听的ip和端口
	if globeCfg.Output.Prometheus {
		go func() {
			fmt.Println("ip", globeCfg.Gw.Addr)
			fmt.Println("port", globeCfg.Gw.HttpListenPort)
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf("%s:%d", globeCfg.Gw.Addr, globeCfg.Gw.HttpListenPort), nil)

		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("exit", s)

}
