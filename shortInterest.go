//This utility program downloads and extracts short volume data from CBOE and FINRA.
//Stephen Penkov 1/10/21
package main

import (
	"fmt"
  "os"
  "log"
  "strings"
  "strconv"
	"io"
  "io/ioutil"
	"net/http"
	"time"
	"archive/zip"
	"path/filepath"
	"bufio"
	"errors"
)

var (
  nameOfExchanges = make(map[string]string)
	market = make(map[string]string)
)

func init(){
  nameOfExchanges["z"] = "BZX"
  nameOfExchanges["y"] = "BYX"
  nameOfExchanges["j"] = "EDGA"
  nameOfExchanges["k"] = "EDGX"
  nameOfExchanges["n"] = "NYSE TRF"
  nameOfExchanges["q"] = "NASDAQ TRF Carteret"
  nameOfExchanges["b"] = "NASDAQ TRF Chicago"
  nameOfExchanges["d"] = "ADF"
	market["BZX"] = "BATS"
	market["BYX"] = "BYXX"
	market["EDGA"] = "EDGA"
	market["EDGX"] = "EDGX"

}


func main(){
  //print the name of the files in the directory
	GetFINRAFiles()
	GetCBOEFiles()

	for{
		fmt.Println("\n\nEnter ticker to retrieve short volume information or :q to quit.")
	  var s string
	  fmt.Scanf("%s",&s)
	  if s == ":q"{
	    os.Exit(1)
	  }
		GetData(s)
	}

}

func GetData(s string){
	files, err := ioutil.ReadDir("./cboe/")
  if err != nil{
    log.Fatal(err)
  }

  for _, file := range files{
    log.Println(file.Name())
    getShortInterestCBOE(s, fmt.Sprintf("%s","./cboe/" + file.Name()))
  }

  files, err = ioutil.ReadDir("./finra/")
  if err != nil{
    log.Fatal(err)
  }
  for _, file := range files{
    //fmt.Println(file.Name())
    GetFINRAShortInterest(s, fmt.Sprintf("%s","./finra/" + file.Name()))
  }
}

func GetFINRAShortInterest(ticker, fileName string) {

	file, err := os.Open(fileName)
  if err != nil{
    log.Fatal(err)
  }
  defer file.Close()

	siData := make(map[string][]int)
  exchanges := make(map[string]string)

	//Read first line
	in := bufio.NewReader(file)
  line, err := in.ReadString('\n')

	var date string
  for{

    var n int
    n, err := fmt.Fscanf(in, "%s\n", &line)
    row := strings.Split(line, "|")
    if len(row) != 6{
      break
    }
    if n == 0 || err != nil{
      log.Println(err)
      break
    }

		date = string(row[0])
    symbol := string(row[1])
    shortVolume,_ := strconv.Atoi(row[2])
    shortExemptVolume,_ := strconv.Atoi(row[3])
    totalVolume,_ := strconv.Atoi(row[4])
    exchanges[strings.ToLower(symbol)] = string(row[5])

    siData[strings.ToLower(symbol)] = append(siData[strings.ToLower(symbol)],shortVolume)
    siData[strings.ToLower(symbol)] = append(siData[strings.ToLower(symbol)],shortExemptVolume)
    siData[strings.ToLower(symbol)] = append(siData[strings.ToLower(symbol)],totalVolume)

  }

  //Retrieve values if the key exists
  for{

    data, ok := siData[strings.ToLower(ticker)]
    if !ok{
      fmt.Println("Not Found.")
      break
    }

    exchangesTraded, ok := exchanges[strings.ToLower(ticker)]
    if !ok{
      fmt.Println("Not Found.")
      break
    }

    fmt.Printf("\n%s Reporting short interest data for exchange: %s\n",date, exchangesTraded)
    fmt.Println("Short Volume: ", data[0])
    fmt.Println("Short Exempt Volume: ", data[1])
    fmt.Println("Total Volume: ", data[2])
    fmt.Printf("Percent short: %.2f%%\n", percentShort(data[0] + data[1],data[2]))
    return
  }
}

func getShortInterestCBOE(ticker, fileName string) {

	file, err := os.Open(fileName)
  if err != nil{
    log.Fatal(err)
  }
  defer file.Close()


  siData := make(map[string][]int)
  exchanges := make(map[string]string)

	//Read first line
	in := bufio.NewReader(file)
  line, err := in.ReadString('\n')


	var date string
  for{

    //Date|Symbol|ShortVolume|TotalVolume|Market
    var n int
    n, err := fmt.Fscanf(in, "%s\n", &line)
    row := strings.Split(line, "|")
    if len(row) != 5{
      break
    }
    if n == 0 || err != nil{
      //log.Println(err)
      break
    }
		date = string(row[0])
    symbol := string(row[1])
    shortVolume,_ := strconv.Atoi(row[2])
    totalVolume,_ := strconv.Atoi(row[3])
    exchanges[strings.ToLower(symbol)] = string(row[4])

    siData[strings.ToLower(symbol)] = append(siData[strings.ToLower(symbol)],shortVolume)
    siData[strings.ToLower(symbol)] = append(siData[strings.ToLower(symbol)],totalVolume)

  }

  //Retrieve values if the key exists
  for{

    data, ok := siData[strings.ToLower(ticker)]
    if !ok{
      fmt.Println("Not Found.")
      break
    }

    exchangesTraded, ok := exchanges[strings.ToLower(ticker)]
    if !ok{
      fmt.Println("Not Found.")
      break
    }

    fmt.Printf("\n%s Reporting short interest data for exchange: %s\n",date, nameOfExchanges[strings.ToLower(exchangesTraded)])
    fmt.Println("Short Volume: ", data[0])
    fmt.Println("Total Volume: ", data[1])
		fmt.Printf("Percent short: %.2f%%\n", percentShort(data[0],data[1]))
    return
  }
}

func GetCBOEFiles(){
	for key, _ := range market{
		getCBOEFiles(key, time.Now().Local().Day())
	}
}

func getCBOEFiles(exchangeName string, day int){
	currentTime := time.Now().Local()
	//https://www.cboe.com/us/equities/market_statistics/short_sale/2022/01/BYXXshvol20220107.txt.zip-dl?mkt=byx
	//https://www.cboe.com/us/equities/market_statistics/short_sale/2022/01/BATSshvol20220107.txt.zip-dl?mkt=bzx
	//https://www.cboe.com/us/equities/market_statistics/short_sale/2022/01/EDGAshvol20220107.txt.zip-dl?mkt=edga
	//https://www.cboe.com/us/equities/market_statistics/short_sale/2022/01/EDGXshvol20220107.txt.zip-dl?mkt=edgx
	timeString := fmt.Sprintf("%d%s%s",currentTime.Year(),getMonth(currentTime),getDay(day))
	fileName := fmt.Sprintf("./cboe/%sshvol%s.zip", exchangeName, timeString)
	txtFile := fmt.Sprintf("./cboe/%sshvol%s.txt", exchangeName, timeString)

	exists,err := Exists(txtFile)
	if exists{
		return
	}

	reqUrl := fmt.Sprintf("https://www.cboe.com/us/equities/market_statistics/short_sale/%d/%s/%sshvol%s.txt.zip-dl?mkt=%s",
		currentTime.Year(),
		getMonth(currentTime),
		market[exchangeName],
		timeString,
		strings.ToLower(exchangeName))

	res, err := http.Get(reqUrl)
	if err != nil{
		log.Panicln(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200{
		//log.Println(reqUrl, ":", 	res.StatusCode)
		getCBOEFiles(exchangeName,day - 1 )
		return
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil{
	  log.Fatal(err)
	}

	//copy file to the appropriate directory
	err = ioutil.WriteFile(fileName, body, 0666)
	if err != nil {
		log.Fatal(err)
	}
	unzipFiles(fileName, "./cboe")
}

func unzipFiles(src,dstDir string){
	zipReader, err := zip.OpenReader(src)
	if err != nil{
		log.Fatal(err)
	}
	defer zipReader.Close()
	//Iterate through archive and copy src files to new dst
	for _, file := range zipReader.Reader.File{
		zippedFile, err := file.Open()
		if err != nil{
			log.Fatal(err)
		}
		defer zippedFile.Close()

		target := fmt.Sprintf("./%s",dstDir)
		extractedFilePath := filepath.Join(target, file.Name)

		if file.FileInfo().IsDir(){
			//log.Println("Creating directory: ", extractedFilePath)
			os.MkdirAll(extractedFilePath, file.Mode())

		}else{
			//log.Println("Extracting file: ", file.Name)
			outputFile, err := os.OpenFile(extractedFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC,file.Mode())
			if err != nil{
				log.Fatal(err)
			}
			defer outputFile.Close()
			_, err = io.Copy(outputFile, zippedFile)
			if err != nil{
				log.Fatal(err)
			}
		}
	}
	//Remove archive
	err = os.Remove(src)
	if err != nil{
		log.Fatal(err)
	}
}

func GetFINRAFiles(){
	getFINRAFiles(time.Now().Local().Day())
}
func getFINRAFiles(day int){
	//Get most recent files
		currentTime := time.Now().Local()
		timeString := fmt.Sprintf("%d%s%s",currentTime.Year(),getMonth(currentTime),getDay(day))
		txtFile := fmt.Sprintf("./finra/CNMSshvol%s.txt", timeString)

		exists,err := Exists(txtFile)
		if exists{
			return
		}

		reqUrl := fmt.Sprintf("https://cdn.finra.org/equity/regsho/daily/CNMSshvol%s.txt",timeString)

		res, err := http.Get(reqUrl)
		if err != nil{
			log.Panicln(err)
		}
		defer res.Body.Close()

		if res.StatusCode != 200{
		//	log.Println(reqUrl, ":", 	res.StatusCode)
			getFINRAFiles(day - 1)
			return
		}
    body, err := ioutil.ReadAll(res.Body)
    if err != nil{
      log.Fatal(err)
    }

		//copy file to the appropriate directory
		fileName := fmt.Sprintf("./finra/CNMSshvol%s.txt", timeString)
		err = ioutil.WriteFile(fileName, body, 0666)
		if err != nil {
			log.Fatal(err)
		}

}

//Utility functions
func Exists(name string) (bool, error) {
    _, err := os.Stat(name)
    if err == nil {
        return true, nil
    }
    if errors.Is(err, os.ErrNotExist) {
        return false, nil
    }
    return false, err
}
func getMonth(t time.Time) string{
	if t.Month() < 10{
		return fmt.Sprintf("0%d",t.Month())
	}
	return fmt.Sprintf("%d",t.Month())
}
func getDay(day int) string{
	if day < 10{
		return fmt.Sprintf("0%d",day)
	}
	return fmt.Sprintf("%d",day)
}

func percentShort(sVolume, tVolume int)float64{
	return (100 * (float64(sVolume) / float64(tVolume)))
}
