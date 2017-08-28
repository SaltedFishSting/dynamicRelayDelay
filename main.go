package main

import (
	"container/list"
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
	nodeh            *(prometheus.HistogramVec)
	nodes            *(prometheus.SummaryVec)
	nodehlosspackage *(prometheus.HistogramVec)
	nodeslosspackage *(prometheus.SummaryVec)
	nodehe2eDelay    *(prometheus.HistogramVec)
	nodese2eDelay    *(prometheus.SummaryVec)
	nodehe2eLoss     *(prometheus.HistogramVec)
	nodese2eLoss     *(prometheus.SummaryVec)
)

//relay节点延迟
func Observe(id string, ip string, direction string, delay float64) {
	intid, _ := strconv.ParseInt(id, 10, 64)

	if delay != 0 && ip == relayidMap[intid] {
		nodeh.WithLabelValues(id, ip, direction).Observe(delay)
		nodes.WithLabelValues(id, ip, direction).Observe(delay)
	}
}

//relay节点丢包
func Observeloss(id string, ip string, direction string, aloss float64, vloss float64) {
	intid, _ := strconv.ParseInt(id, 10, 64)
	if ip == relayidMap[intid] {
		if aloss < 2000 && aloss >= 0 {
			nodehlosspackage.WithLabelValues(id, ip, direction, "audio").Observe(aloss)
			nodeslosspackage.WithLabelValues(id, ip, direction, "audio").Observe(aloss)
		} else {
			fmt.Println(aloss)
		}
		if vloss < 10000 && vloss >= 0 {
			nodehlosspackage.WithLabelValues(id, ip, direction, "video").Observe(vloss)
			nodeslosspackage.WithLabelValues(id, ip, direction, "video").Observe(vloss)
		} else {
			fmt.Println(vloss)
		}
	}
}

//端到端延迟
func Observee2edelay(strtype string, delay float64) {
	nodehe2eDelay.WithLabelValues(strtype).Observe(delay)
	nodese2eDelay.WithLabelValues(strtype).Observe(delay)
}

//端到端丢包
func Observee2eloss(strtype string, aloss float64, vloss float64) {
	if aloss < 2000 && aloss >= 0 {
		nodehe2eLoss.WithLabelValues(strtype, "audio").Observe(aloss)
		nodese2eLoss.WithLabelValues(strtype, "audio").Observe(aloss)
	} else {
		fmt.Println(aloss)
	}
	if vloss < 10000 && vloss >= 0 {
		nodehe2eLoss.WithLabelValues(strtype, "video").Observe(vloss)
		nodese2eLoss.WithLabelValues(strtype, "video").Observe(vloss)
	} else {
		fmt.Println(vloss)
	}
}

//从mongedb获取以解码的callBaseLog数据数组
func mongodbToBaselog(ip string, db string, table string) []BaseLog {

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
	return BaseLogresult

}
func toPromtheus(BaseLogresult []BaseLog) {
	sidgroup := make(map[string]list.List)
	for _, result := range BaseLogresult {
		if a := sidgroup[result.Sid]; a.Len() == 0 {
			if len(result.CallBaseLog) != 0 {
				sidlist := list.New()
				sidlist.PushBack(result.CallBaseLog)
				sidgroup[result.Sid] = *sidlist
			}
		} else {
			if len(result.CallBaseLog) != 0 {
				sidlist := sidgroup[result.Sid]
				sidlist.PushBack(result.CallBaseLog)
				sidgroup[result.Sid] = sidlist
			}
		}
	}

	for sid, result := range sidgroup {
		cidgroup := make(map[string]list.List)
		for e := result.Front(); e != nil; e = e.Next() {
			if e.Value == nil {
				continue
			}
			for _, v := range e.Value.(map[string]string) {
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
						//****
						if d, _ := strconv.ParseFloat(smap["delay_aver"], 64); d > 20000 {
							continue
						}
						smap["sid"] = sid
						if l := cidgroup[smap["cid"]]; l.Len() == 0 {
							cidlist := list.New()
							cidlist.PushBack(smap)
							cidgroup[smap["cid"]] = *cidlist
						} else {
							cidlist := cidgroup[smap["cid"]]
							cidlist.PushBack(smap)
							cidgroup[smap["cid"]] = cidlist
						}
					}
				}
			}
		}
		for _, v := range cidgroup {
			var e2eloss [500]map[string]float64
			var CLUloss [500]map[string]string
			var CRDloss [500]map[string]string
			var CRUloss [500]map[string]string
			var CLDloss [500]map[string]string
			for e := v.Front(); e != nil; e = e.Next() {
				if e.Value == nil {
					continue
				}
				smap := e.Value.(map[string]string)
				switch smap["sub_type"] {
				case "CLU":
					if strings.Index(smap["dst"], "_") >= 0 {
						a, _ := strconv.ParseFloat(smap["delay_aver"], 64)
						index, _ := strconv.Atoi(smap["logIndex"])

						ipidlogindex := strings.Split(smap["dst"], "_")
						if ipidlogindex[1] != "-1" {
							Observe(ipidlogindex[1], ipidlogindex[0], "Down", a)
						}
						if index < 500 {
							maploss := make(map[string]string)
							maploss["audio"] = smap["a_loss"]
							maploss["video"] = smap["v_loss"]
							maploss["id"] = ipidlogindex[1]
							maploss["ip"] = ipidlogindex[0]
							CLUloss[index] = maploss
						}
					}

				case "CRD":

					if strings.Index(smap["src"], "_") >= 0 {
						a, _ := strconv.ParseFloat(smap["delay_aver"], 64)

						index, _ := strconv.Atoi(smap["logIndex"])

						ipidlogindex := strings.Split(smap["src"], "_")
						if ipidlogindex[1] != "-1" {
							Observe(ipidlogindex[1], ipidlogindex[0], "Up", a)
						}
						if index < 500 {
							maploss := make(map[string]string)
							maploss["audio"] = smap["a_loss"]
							maploss["video"] = smap["v_loss"]
							maploss["id"] = ipidlogindex[1]
							maploss["ip"] = ipidlogindex[0]
							CRDloss[index] = maploss
						}
					}

				case "CRU":

					if strings.Index(smap["dst"], "_") >= 0 {
						a, _ := strconv.ParseFloat(smap["delay_aver"], 64)

						index, _ := strconv.Atoi(smap["logIndex"])

						ipidlogindex := strings.Split(smap["dst"], "_")
						if ipidlogindex[1] != "-1" {
							Observe(ipidlogindex[1], ipidlogindex[0], "Down", a)
						}
						if index < 500 {
							maploss := make(map[string]string)
							maploss["audio"] = smap["a_loss"]
							maploss["video"] = smap["v_loss"]
							maploss["id"] = ipidlogindex[1]
							maploss["ip"] = ipidlogindex[0]
							CRUloss[index] = maploss
						}
					}

				case "CLD":

					if strings.Index(smap["src"], "_") >= 0 {

						a, _ := strconv.ParseFloat(smap["delay_aver"], 64)

						index, _ := strconv.Atoi(smap["logIndex"])

						ipidlogindex := strings.Split(smap["src"], "_")
						if ipidlogindex[1] != "-1" {
							Observe(ipidlogindex[1], ipidlogindex[0], "Up", a)
						}
						if index < 500 {
							maploss := make(map[string]string)
							maploss["audio"] = smap["a_loss"]
							maploss["video"] = smap["v_loss"]
							maploss["id"] = ipidlogindex[1]
							maploss["ip"] = ipidlogindex[0]
							CLDloss[index] = maploss
						}
					}
				case "CE2E_L2R":
					delay, _ := strconv.ParseFloat(smap["delay_aver"], 64)
					if delay != 0 && delay < 20000 {
						Observee2edelay("e2e", delay)
					}

				case "CE2E_R2L":
					delay, _ := strconv.ParseFloat(smap["delay_aver"], 64)
					if delay != 0 && delay < 20000 {
						Observee2edelay("e2e", delay)
					}

				case "CSR":
					aloss, _ := strconv.ParseFloat(smap["a_loss"], 64)
					vloss, _ := strconv.ParseFloat(smap["v_after_fec_recover_loss"], 64)
					index, _ := strconv.Atoi(smap["logIndex"])
					if index < 500 {
						maploss := make(map[string]float64)
						maploss["audio"] = aloss
						maploss["video"] = vloss
						e2eloss[index] = maploss
					}

				}

			}
			acmap := make(map[string]float64)
			for _, v := range e2eloss {
				if v != nil {
					if len(acmap) == 0 {
						acmap = v
						Observee2eloss("e2e", v["audio"], v["video"])
					} else {
						Observee2eloss("e2e", v["audio"]-acmap["audio"], v["video"]-acmap["video"])
						acmap = v
					}

				}
			}

			cluacmap := make(map[string]string)
			for _, v := range CLUloss {
				if v != nil {
					aloss, _ := strconv.ParseFloat(v["audio"], 64)
					vloss, _ := strconv.ParseFloat(v["video"], 64)
					if len(cluacmap) == 0 {
						cluacmap = v
						Observeloss(v["id"], v["ip"], "Down", aloss, vloss)
					} else {
						clua, _ := strconv.ParseFloat(cluacmap["audio"], 64)
						cluv, _ := strconv.ParseFloat(cluacmap["video"], 64)
						Observeloss(v["id"], v["ip"], "Down", aloss-clua, vloss-cluv)
						cluacmap = v
					}

				}
			}
			crdacmap := make(map[string]string)
			for _, v := range CRDloss {
				if v != nil {
					aloss, _ := strconv.ParseFloat(v["audio"], 64)
					vloss, _ := strconv.ParseFloat(v["video"], 64)
					if len(crdacmap) == 0 {
						crdacmap = v
						Observeloss(v["id"], v["ip"], "Up", aloss, vloss)
					} else {
						crda, _ := strconv.ParseFloat(crdacmap["audio"], 64)
						crdv, _ := strconv.ParseFloat(crdacmap["video"], 64)
						Observeloss(v["id"], v["ip"], "Up", aloss-crda, vloss-crdv)

						crdacmap = v
					}

				}
			}
			cruacmap := make(map[string]string)
			for _, v := range CRUloss {
				if v != nil {
					aloss, _ := strconv.ParseFloat(v["audio"], 64)
					vloss, _ := strconv.ParseFloat(v["video"], 64)
					if len(cruacmap) == 0 {
						cruacmap = v
						Observeloss(v["id"], v["ip"], "Down", aloss, vloss)
					} else {
						crua, _ := strconv.ParseFloat(cruacmap["audio"], 64)
						cruv, _ := strconv.ParseFloat(cruacmap["video"], 64)
						Observeloss(v["id"], v["ip"], "Down", aloss-crua, vloss-cruv)

						cruacmap = v
					}

				}
			}
			cldacmap := make(map[string]string)
			for _, v := range CLDloss {
				if v != nil {
					aloss, _ := strconv.ParseFloat(v["audio"], 64)
					vloss, _ := strconv.ParseFloat(v["video"], 64)
					if len(cldacmap) == 0 {
						cldacmap = v
						Observeloss(v["id"], v["ip"], "Up", aloss, vloss)
					} else {
						clda, _ := strconv.ParseFloat(cldacmap["audio"], 64)
						cldv, _ := strconv.ParseFloat(cldacmap["video"], 64)
						Observeloss(v["id"], v["ip"], "Up", aloss-clda, vloss-cldv)

						cldacmap = v
					}
				}
			}
		}

	}
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
	nodehlosspackage = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "H",
		Name:      "relayloss",
		Help:      "nodeh",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"RelayId",
			"IP",
			"direction",
			"content",
		})

	nodeslosspackage = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "S",
		Name:       "relayloss",
		Help:       "nodes",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"RelayId",
			"IP",
			"direction",
			"content",
		})
	nodehe2eDelay = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "H",
		Name:      "e2eDelay",
		Help:      "nodeh",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"type",
		})

	nodese2eDelay = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "S",
		Name:       "e2eDelay",
		Help:       "nodes",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"type",
		})
	nodehe2eLoss = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rt",
		Subsystem: "H",
		Name:      "e2eLoss",
		Help:      "nodeh",
		Buckets:   prometheus.LinearBuckets(HistogramOptsparamMap["start"], HistogramOptsparamMap["width"], int(HistogramOptsparamMap["count"])),
	},
		[]string{
			"type",
			"content",
		})

	nodese2eLoss = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "rt",
		Subsystem:  "S",
		Name:       "e2eLoss",
		Help:       "nodes",
		Objectives: SummaryOptsparamMap,
	},
		[]string{
			"type",
			"content",
		})
	prometheus.MustRegister(nodeh)
	prometheus.MustRegister(nodes)
	prometheus.MustRegister(nodehlosspackage)
	prometheus.MustRegister(nodeslosspackage)
	prometheus.MustRegister(nodehe2eDelay)
	prometheus.MustRegister(nodese2eDelay)
	prometheus.MustRegister(nodehe2eLoss)
	prometheus.MustRegister(nodese2eLoss)

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
			toPromtheus(mongodbToBaselog(ip, db, table))

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
	//	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("asd")
	fmt.Println("exitss", s)

}
