package testing

import (
	"cdd/cdd"
	"fmt"
	"testing"
	"time"
)
type MToken struct {
	Id int `gorm:auto_increment`
	Token string `gorm:varchar(64)`
}

func Test_Create(t *testing.T)  {
	cfgList := getConfg()
	p,err:= cdd.NewCDD(cfgList,true)
	if err != nil {
		fmt.Println("error :",err.Error())
	}
	var listCode []string
	for i:=0;i<10;i++ {
		orderCode := p.GenerateUniqueCode28(int64(i),time.Now())
		conn,err := p.AnalysisUniqueCode28(orderCode.UniqueCode,"m_token")
		if err !=nil {
			fmt.Println(err.Error())
			return
		}
		var mm MToken
		mm.Token = orderCode.UniqueCode
		listCode = append(listCode, orderCode.UniqueCode)
		fmt.Println(conn.TableName)
		db :=conn.Conn.Table(conn.TableName).Create(&mm)
		if db.Error != nil {
			fmt.Println("插入失败 :","table:",conn.TableName," token:",orderCode," cfg:",conn)
		}else{
			fmt.Println("插入成功 :","table:",conn.TableName," token:",orderCode," cfg:",conn)
		}
	}

	{
		//查询
		for _,code :=range listCode{
			var ttt MToken
			token := code
			conn,err := p.AnalysisUniqueCode28(token,"m_token")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			tableName := fmt.Sprintf("m_token_%v",p.SupStr(conn.TableCode,2))
			conn.Conn.Table(tableName).Where("token = ?",token).Find(&ttt)
			fmt.Println(code," 查询结果 : ",ttt)
		}
	}
}




func Test_func(t *testing.T) {
	fgList := getConfg()
	p, err := cdd.NewCDD(fgList, true)
	if err != nil {
		fmt.Println("error :", err.Error())
	}
	{
		fmt.Println("*** 28位")
		r := p.GenerateUniqueCode28(2, time.Now())
		fmt.Println(r)
		fmt.Println(p.AnalysisUniqueCode28(r.UniqueCode, "mytest"))

		fmt.Println("list 批量解析………………")
		var list2 []string
		list2 = append(list2, "2021011510191821310801010000", "2021011510285781610401020001", "2021011510350576747602010002")
		rr, _ := p.AnalysisListUniqueCode28(list2, "mytest")
		for ikey, item := range rr {
			fmt.Println(ikey, ":", item)
		}
	}

	{
		fmt.Println("*** 36位")
		r, _ := p.GenerateUniqueCode36("121", "10000", 2, time.Now())
		fmt.Println(r)
		fmt.Println(p.AnalysisUniqueCode36(r.UniqueCode, "mytest"))
		fmt.Println("list 批量解析………………")
		var list []string
		list = append(list, "121100002021011510494966532001010000", "121100002021011510353294841201020001", "121100002021011510501835328802010002")
		rr, _ := p.AnalysisListUniqueCode36(list, "mytest")
		for ikey, item := range rr {
			fmt.Println(ikey, ":", item)
		}
	}

}

func getConfg() []cdd.DBCONFIG{

	var cfgList []cdd.DBCONFIG
	{
		var cfg1 cdd.DBCONFIG
		cfg1.DbNo = 01
		cfg1.TableNo = 01
		cfg1.Host = "localhost"
		cfg1.Port = "3306"
		cfg1.UserName = "root"
		cfg1.UserPwd = "root"
		cfg1.DbName = "yk_mytoken"
		cfgList = append(cfgList, cfg1)
	}
	{
		var cfg1 cdd.DBCONFIG
		cfg1.DbNo = 01
		cfg1.TableNo = 02
		cfg1.Host = "127.0.0.1"
		cfg1.Port = "3306"
		cfg1.UserName = "root"
		cfg1.UserPwd = "root"
		cfg1.DbName = "yk_mytoken"
		cfgList = append(cfgList, cfg1)
	}

	{
		var cfg1 cdd.DBCONFIG
		cfg1.DbNo = 02
		cfg1.TableNo = 01
		cfg1.Host = "127.0.0.1"
		cfg1.Port = "3306"
		cfg1.UserName = "root"
		cfg1.UserPwd = "root"
		cfg1.DbName = "yk_mytoken02"
		cfgList = append(cfgList, cfg1)
	}

	return cfgList
}

/*
func Test_createOneDb(t *testing.T)  {
	cfgList := getOneConfg()
	p,err:= distributed.NewMyDbDistributed(cfgList)
	if err != nil {
		fmt.Println("error :",err.Error())
	}
	for i:=0;i<10;i++ {
		orderCode := p.Generate(time.Now(),int64(i))
		conn,err := p.AnalysisCode(orderCode,"mytoken")
		if err !=nil {
			fmt.Println(err.Error())
			return
		}
		var mm MToken
		mm.Token = orderCode
		tableName := fmt.Sprintf("m_token_%v",conn.Key)
		db :=conn.Conn.Table(tableName).Create(&mm)
		if db.Error != nil {
			fmt.Println("插入失败 :","table:",tableName," token:",orderCode," x:",conn)
		}else{
			fmt.Println("插入成功 :","table:",tableName," token:",orderCode," x:",conn)
		}
	}
}
*/


func getOneConfg() []cdd.DBCONFIG {
	var cfgList []cdd.DBCONFIG
	{
		var cfg1 cdd.DBCONFIG
		cfg1.DbNo = 01
		cfg1.TableNo = 01
		cfg1.Host = "localhost"
		cfg1.Port = "3306"
		cfg1.UserName = "root"
		cfg1.UserPwd = "root"
		cfg1.DbName = "yk_mytoken"
		cfgList = append(cfgList, cfg1)
	}
	return cfgList
}