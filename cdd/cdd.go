package cdd

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/w3liu/go-common/constant/timeformat"
	"os"
	"strconv"
	"time"
)

const DB_MaxConns = 10
const DB_MaxOpenConns = 15

//配置文件
type DBCONFIG struct {
	Host           string // 主机名称
	Port           string // 端口
	UserName       string // 用户名
	UserPwd        string // 密码
	DbName         string // 数据库名称
	DbNo           int64  //2位   - 库名
	TableNo        int64  //2位   - 表名
	DbMaxConns     int
	DBMaxOpenConns int
}

//分布式服务
type CLOUDDBCONFIG struct {
	Key        string   //2位(库)2位(表名) = key
	TableName  string   //解析采用的到 - table的名称
	UniqueCode string   //解析采用的到 - 需要解析的code
	Conn       *gorm.DB //本数据的CONN
	Cfg        DBCONFIG //配置文件
}

type CDD struct {
	cfgList  []DBCONFIG
	connMap  map[string]CLOUDDBCONFIG
	connlist []CLOUDDBCONFIG
}

//解析 CLOUD - Unique - Code详情
type CLOUDUNIQUECODE struct {
	Prefix      string   //3位前缀      - 	181:会员卡   	121:门店号
	PartnerCode string   //5位商户号
	DateData    string   //14位时间     - 	年月日时分秒
	Millisecond string   //3位毫秒数
	Ps          string   //3位进程号
	DbCode      string   //2位库名
	TableCode   string   //2位表名
	Nom         string   //4位序号
	UniqueCode  string   //生成后的 	云 code
	Conn        *gorm.DB //数据库链接
	TableName   string   //表名
}

func NewCDD(configList []DBCONFIG, isOpen bool) (*CDD, error) {
	p := CDD{}
	p.cfgList = configList
	p.connMap = make(map[string]CLOUDDBCONFIG)
	//循环打开
	for _, cfg := range p.cfgList {
		var item CLOUDDBCONFIG
		item.Cfg = cfg
		if isOpen {
			conn, err := p.OpenDb(cfg)
			if err != nil {
				return nil, err
			}
			item.Conn = conn
			item.Key = fmt.Sprintf("%v%v", p.Sup(cfg.DbNo, 2), p.Sup(cfg.TableNo, 2))
			p.connMap[item.Key] = item
			p.connlist = append(p.connlist, item)
		} else {
			item.Conn = nil
			item.Key = fmt.Sprintf("%v%v", p.Sup(cfg.DbNo, 2), p.Sup(cfg.TableNo, 2))
			p.connMap[item.Key] = item
			p.connlist = append(p.connlist, item)
		}
	}
	return &p, nil
}

func (p *CDD) OpenDb(cfg DBCONFIG) (*gorm.DB, error) {
	CloudDb, err := gorm.Open("mysql", cfg.UserName+":"+cfg.UserPwd+"@tcp("+cfg.Host+")/"+cfg.DbName+"?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		return nil, err
	}
	CloudDb.SingularTable(true)
	CloudDb.DB().SetMaxIdleConns(cfg.DbMaxConns)
	CloudDb.DB().SetMaxOpenConns(cfg.DBMaxOpenConns)
	return CloudDb, nil
}

func (p *CDD) CloseDb(cfg DBCONFIG) {
	keyName := fmt.Sprintf("%v%v", p.Sup(cfg.DbNo, 2), p.Sup(cfg.TableNo, 2))
	if _, ok := p.connMap[keyName]; ok {
		if p.connMap[keyName].Conn != nil {
			p.connMap[keyName].Conn.Close()
			//p.connlist[keyName].Conn = nil
		}
	}
}

func (p *CDD) CloseDbAll() {
	for _, conn := range p.connMap {
		if conn.Conn != nil {
			p.CloseDb(conn.Cfg)
		}
	}
}

//生成24位订单号
// num 为序号
//前面17位代表时间精确到毫秒，中间3位代表进程id，    (22位)2位表名     序号最后4位代表序号
// 20201231161128|753|164|02|01|0002
func (p *CDD) GenerateUniqueCode28(num int64, t time.Time) CLOUDUNIQUECODE {
	var code CLOUDUNIQUECODE
	{ //time
		code.DateData = t.Format(timeformat.Continuity)
		m := t.UnixNano()/1e6 - t.UnixNano()/1e9*1e3
		code.Millisecond = p.Sup(m, 3)
	}
	{ //pid
		pid := os.Getpid() % 1000
		code.Ps = p.Sup(p.pidSub(int64(pid)), 3)
	}
	{
		qm := num % int64(len(p.connMap))
		db := p.connlist[qm]
		code.DbCode = p.Sup(db.Cfg.DbNo, 2)
		code.TableCode = p.Sup(db.Cfg.TableNo, 2)
	}
	code.Nom = p.Sup(num, 4)
	code.UniqueCode = fmt.Sprintf("%s%s%s%s%s%s", code.DateData, code.Millisecond, code.Ps, code.DbCode, code.TableCode, code.Nom)
	return code
}

//分析出表名
func (p *CDD) AnalysisUniqueCode28(UniqueCode string, tableName string) (*CLOUDUNIQUECODE, error) {
	if len(UniqueCode) != 28 {
		return nil, errors.New("UniqueCode长度必须为28位数")
	}
	var resultCode CLOUDUNIQUECODE
	resultCode.Prefix = ""
	resultCode.PartnerCode = ""
	resultCode.DateData = UniqueCode[0:14]
	resultCode.Millisecond = UniqueCode[14:17]
	resultCode.Ps = UniqueCode[17:20]
	resultCode.DbCode = UniqueCode[20:22]
	resultCode.TableCode = UniqueCode[22:24]
	resultCode.Nom = UniqueCode[24:28]
	resultCode.Conn = nil
	resultCode.UniqueCode = UniqueCode
	key := fmt.Sprintf("%v%v", resultCode.DbCode, resultCode.TableCode)
	if _, ok := p.connMap[key]; ok {
		rConn := p.connMap[key]
		resultCode.TableName = fmt.Sprintf("%v_%v", tableName, p.Sup(rConn.Cfg.TableNo, 2))
		resultCode.Conn = rConn.Conn
	}
	return &resultCode, nil
}

func (p *CDD) AnalysisListUniqueCode28(listUniqueCode []string, tableName string) (map[string][]*CLOUDUNIQUECODE, error) {
	var resultList map[string][]*CLOUDUNIQUECODE
	resultList = make(map[string][]*CLOUDUNIQUECODE)
	for _, item := range listUniqueCode {
		d, err := p.AnalysisUniqueCode28(item, tableName)
		if err != nil {
			fmt.Println(err.Error())
			return resultList, err
		}
		key := fmt.Sprintf("%v%v", d.DbCode, d.TableCode)
		resultList[key] = append(resultList[key], d)
	}
	return resultList, nil
}

//生成 CloudOrder
func (p *CDD) GenerateUniqueCode36(prefix string, partnerCode string, num int64, t time.Time) (*CLOUDUNIQUECODE, error) {
	if len(prefix) != 3 {
		return nil, errors.New("前缀长度必须位3位")
	}
	if len(partnerCode) > 5 {
		return nil, errors.New("商户号不能大于5位")
	}
	var code CLOUDUNIQUECODE
	code.Prefix = p.SupStr(prefix, 3)
	code.PartnerCode = p.SupStr(partnerCode, 5)
	{ //time
		code.DateData = t.Format(timeformat.Continuity)
		m := t.UnixNano()/1e6 - t.UnixNano()/1e9*1e3
		code.Millisecond = p.Sup(m, 3)
	}
	{ //pid
		pid := os.Getpid() % 1000
		code.Ps = p.Sup(p.pidSub(int64(pid)), 3)
	}
	{
		qm := num % int64(len(p.connMap))
		db := p.connlist[qm]
		code.DbCode = p.Sup(db.Cfg.DbNo, 2)
		code.TableCode = p.Sup(db.Cfg.TableNo, 2)
	}
	code.Nom = p.Sup(num, 4)
	code.UniqueCode = fmt.Sprintf("%s%s%s%s%s%s%s%s", code.Prefix, code.PartnerCode, code.DateData, code.Millisecond, code.Ps, code.DbCode, code.TableCode, code.Nom)
	return &code, nil
}

// 解析 cloudCode
func (p *CDD) AnalysisUniqueCode36(cloudCodeStr string, tableName string) (*CLOUDUNIQUECODE, error) {
	if len(cloudCodeStr) != 36 {
		return nil, errors.New("CloudCode必须为36位")
	}
	var resultCloudCode CLOUDUNIQUECODE
	resultCloudCode.Prefix = cloudCodeStr[0:3]
	resultCloudCode.PartnerCode = cloudCodeStr[3:8]
	resultCloudCode.DateData = cloudCodeStr[8:22]
	resultCloudCode.Millisecond = cloudCodeStr[22:25]
	resultCloudCode.Ps = cloudCodeStr[25:28]
	resultCloudCode.DbCode = cloudCodeStr[28:30]
	resultCloudCode.TableCode = cloudCodeStr[30:32]
	resultCloudCode.Nom = cloudCodeStr[32:36]
	resultCloudCode.UniqueCode = cloudCodeStr
	resultCloudCode.Conn = nil
	key := fmt.Sprintf("%v%v", resultCloudCode.DbCode, resultCloudCode.TableCode)
	if _, ok := p.connMap[key]; ok {
		rConn := p.connMap[key]
		resultCloudCode.TableName = fmt.Sprintf("%v_%v", tableName, p.Sup(rConn.Cfg.TableNo, 2))
		resultCloudCode.Conn = rConn.Conn
	}
	return &resultCloudCode, nil
}

func (p *CDD) AnalysisListUniqueCode36(listCloudCodeStr []string, tableName string) (map[string][]*CLOUDUNIQUECODE, error) {
	var resultList map[string][]*CLOUDUNIQUECODE
	resultList = make(map[string][]*CLOUDUNIQUECODE)
	for _, item := range listCloudCodeStr {
		d, err := p.AnalysisUniqueCode36(item, tableName)
		if err != nil {
			fmt.Println(err.Error())
			return resultList, err
		}
		key := fmt.Sprintf("%v%v", d.DbCode, d.TableCode)
		resultList[key]= append(resultList[key],d)
	}
	return resultList, nil
}

func (p *CDD) pidSub(i int64) int64 {
	if i > 999 {
		str := fmt.Sprintf("%d", i)
		iStr := string([]byte(str)[0:3])
		i, _ = strconv.ParseInt(iStr, 10, 64)
	}
	return i
}

//对长度不足n的数字前面补0
func (p *CDD) Sup(i int64, n int) string {
	m := fmt.Sprintf("%d", i)
	for len(m) < n {
		m = fmt.Sprintf("0%s", m)
	}
	return m
}
func (p *CDD) SupStr(str string, n int) string {
	for len(str) < n {
		str = fmt.Sprintf("0%s", str)
	}
	return str
}
