package main

// TODO: 등록확인 되기 전에는 PM,SM 전송 불가하게 만들 것, 실행파일로 만든 후, AWS 이용해서 여러개 마이크로서비스 상태에서 테스트 해볼것

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
)

const (
	KGM = 1 + iota
	KSM
	PM
	SM
	RM
	WM
)

type Message struct {
	From    string //ip 주소
	Version string
	Time    string
	Kind    int //종류
}

type MsgUnit interface {
	// ConvertToJson - send 전 json 형식으로 바꾸는 함수
	ConvertToJson() ([]byte, error)
	// CheckType - Message 의 타입을 알려줌
	CheckType() int
}

type RegisterImgMsg struct {
	Message
	PrivateKey float64
}

type RegisterMsg struct {
	Message
	PrivateKey int64
}

type SubscriptionImage struct {
	Message
	Topic []float64 //
}

type SubscriptionMsg struct {
	Message
	Topic    []int64  //대주제
	Value    []int64  //피연산자
	Operator []string //연산자
	IsAlpha  bool     //value가 숫자인지 문자열인지
}

type ReceivedPubImage struct {
	PublishImage
	Cosine []float64
}

type PublishImage struct {
	Message
	Topic []float64 // 이미지 vector
}
type PublishMsg struct {
	Message
	Topic   []int64 //대주제
	Value   []int64 //Topic 의 세부적인 내용
	Content []int64 // 내용
}

type WithdrawMsg struct {
	Message
}

//type ImgReceiver struct {
//	thisNodeAddr string
//	microService MicroImgService
//}

type Receiver struct {
	thisNodeAddr string
	microService MicroService
}

//type MicroImgService struct {
//	ClientAddr string
//	PrivateKey float64
//	ShareKey   float64
//	// 메세지 미들웨어에 연결되어 있는가
//	IsConnected chan bool
//	MMAddress   string
//}

type MicroService struct {
	ClientAddr string
	PrivateKey float64
	ShareKey   float64
	// 메세지 미들웨어에 연결되어 있는가
	IsConnected chan bool
	MMAddress   string
}

func (receiver Receiver) Receive(args Args, reply *Reply) error {
	switch args.Kind {

	case KSM:
		//var msg KeyShareMsg
		//err := json.Unmarshal(args.JsonMsg, &msg)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	_, err := receiver.moscato.Receive(msg)
		//	if err != nil {
		//
		//	}
		//}()
		//reply.CompleteLog = "received completely"
	case PM:
		var msg ReceivedPubImage
		err := json.Unmarshal(args.JsonMsg, &msg)
		if err != nil {
			return err
		}
		go func() {
			receiver.microService.Receive(msg)
		}()
		reply.CompleteLog = "[" + receiver.thisNodeAddr + "] : pubMsg received"
		//var msg PublishMsg
		//err := json.Unmarshal(args.JsonMsg, &msg)
		//if err != nil {
		//	return err
		//}
		//
		//go func() {
		//	_, err := receiver.moscato.Receive(msg)
		//	if err != nil {
		//
		//	}
		//}()
		//reply.CompleteLog = "PM received completely"
	case SM:
		//var msg SubscriptionMsg
		//err := json.Unmarshal(args.JsonMsg, &msg)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	_, err := receiver.moscato.Receive(msg)
		//	if err != nil {
		//
		//	}
		//}()
		//reply.CompleteLog = "received completely"
	case RM:
		var msg RegisterMsg

		err := json.Unmarshal(args.JsonMsg, &msg)
		if err != nil {
			return err
		}
		go func() {
			receiver.microService.Receive(msg)
		}()
		reply.CompleteLog = "[" + receiver.thisNodeAddr + "] : ackRM received"
	case WM:
		//var msg WithdrawMsg
		//err := json.Unmarshal(args.JsonMsg, &msg)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	_, err := receiver.moscato.Receive(msg)
		//	if err != nil {
		//
		//	}
		//}()
		//reply.CompleteLog = "received completely"
	default:
		return errors.New("message type Error: Not registered message type")
	}
	//reply.CompleteLog = "received completely"
	return nil
}

func (microService MicroService) Receive(msg MsgUnit) {
	logger := NewMyLogger()
	logger.Sync()
	fmt.Println(msg)
	var origin_msg = OriginMsg
	var msg_type = msg.CheckType()
	//메세지 타입에 따라 다르게 처리
	switch msg_type {

	case KSM: //Key share msg

	case PM: //Publish msg
		pmsg := msg.(ReceivedPubImage).PublishImage
		cosine := msg.(ReceivedPubImage).Cosine
		fmt.Println("----- received PM ------")
		DecryptionImg(pmsg, origin_msg, microService.ShareKey, microService.PrivateKey)
		fmt.Println("Topic is", FloatSlice2String(pmsg.Topic))
		fmt.Println("Similarity is ", cosine)
		fmt.Println("Address is", pmsg.From)
		fmt.Println("------------------------")

	case SM: //Subscription msg

	case RM: //Register msg

		microService.IsConnected <- true
		logger.Info("MM Registered this node Complete!")

	case WM: //Withdraw msg
		//moscato.MicroServiceManager.RemoveMicroservice(msg.(*WithdrawMsg).From)

	default:
		errors.New("message type Error: Not registered message type")
	}

	return
}

func (msg RegisterImgMsg) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg SubscriptionImage) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg SubscriptionMsg) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg PublishImage) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg PublishMsg) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg RegisterMsg) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg WithdrawMsg) ConvertToJson() ([]byte, error) {
	js := msg
	jsonBytes, err := json.Marshal(js)
	return jsonBytes, err
}

func (msg Message) CheckType() int {
	return msg.Kind
}

func CreatePubImage(msg Message, topic string) PublishImage {

	publishTextSlice := strings.Split(topic, " ")
	var topicFloat []float64
	for index := 0; index < len(publishTextSlice); index++ {
		float, _ := strconv.ParseFloat(publishTextSlice[index], 8)
		topicFloat = append(topicFloat, float)
	}
	return PublishImage{msg, topicFloat}
}
func CreatePubMsg(msg Message, topic string, value string, content string) PublishMsg {
	//toPubMsg := new(PublishMsg)
	//toPubMsg.Message = msg

	intArr := []rune(topic)
	//fmt.Print("Topic length ")
	//fmt.Println(len(intArr))
	//fmt.Println(len(toPubMsg.Topic))
	var topicInt []int64
	for index := 0; index < len(intArr); index++ {
		//toPubMsg.Topic = append(toPubMsg.Topic, int64(intArr[index]))
		topicInt = append(topicInt, int64(intArr[index]))
	}
	//fmt.Println(len(toPubMsg.Topic))

	intArr = []rune(value)
	var valueInt []int64
	strInt64, _ := strconv.ParseInt(value, 10, 64)
	if unicode.IsDigit(intArr[0]) {
		valueInt = append(valueInt, strInt64)
	} else {
		for index := 0; index < len(intArr); index++ {
			//toPubMsg.Value = append(toPubMsg.Value, int64(intArr[index]))
			valueInt = append(valueInt, int64(intArr[index]))
		}
	}

	intArr = []rune(content)
	var contentInt []int64
	for index := 0; index < len(intArr); index++ {
		//toPubMsg.content = append(toPubMsg.content, int64(intArr[index]))
		contentInt = append(contentInt, int64(intArr[index]))
	}

	return PublishMsg{msg, topicInt, valueInt, contentInt}
}
func CreateSubImage(msg Message, topic string) SubscriptionImage {

	publishTextSlice := strings.Split(topic, " ")
	var topicFloat []float64
	for index := 0; index < len(publishTextSlice); index++ {
		float, _ := strconv.ParseFloat(publishTextSlice[index], 8)
		topicFloat = append(topicFloat, float)
	}
	return SubscriptionImage{msg, topicFloat}
}

func CreateSubMsg(msg Message, topic string, value string, operator string, isAlpha bool) SubscriptionMsg {
	//toPubMsg := new(PublishMsg)
	//toPubMsg.Message = msg

	intArr := []rune(topic)
	//fmt.Print("Topic length ")
	//fmt.Println(len(intArr))
	//fmt.Println(len(toPubMsg.Topic))
	var topicInt []int64
	for index := 0; index < len(intArr); index++ {
		//toPubMsg.Topic = append(toPubMsg.Topic, int64(intArr[index]))
		topicInt = append(topicInt, int64(intArr[index]))
	}
	//fmt.Println(len(toPubMsg.Topic))
	var valueInt []int64
	if isAlpha {
		intArr = []rune(value)

		for index := 0; index < len(intArr); index++ {
			//toPubMsg.Value = append(toPubMsg.Value, int64(intArr[index]))
			valueInt = append(valueInt, int64(intArr[index]))
		}
	} else {
		stringSlice := strings.Split(value, " ")
		for index := 0; index < len(stringSlice); index++ {
			strInt64, _ := strconv.ParseInt(stringSlice[index], 10, 64)
			valueInt = append(valueInt, strInt64)
		}
	}
	var operatorSlice []string
	operatorSlice = strings.Split(operator, " ")

	//intArr = []rune(content)
	//var contentInt [] int64
	//for index := 0; index < len(intArr); index++ {
	//	//toPubMsg.content = append(toPubMsg.content, int64(intArr[index]))
	//	contentInt = append(contentInt, int64(intArr[index]))
	//}

	return SubscriptionMsg{msg, topicInt, valueInt, operatorSlice, isAlpha}
}

type Args struct { // 매개변수
	JsonMsg []byte
	Kind    int
}

type Reply struct { // 리턴값
	CompleteLog string
}

func generateRanUint64() uint64 {
	rand.Seed(time.Now().UnixNano())
	//return uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
	return uint64(0 + uint64(rand.Uint32()))
}

func main() {
	logger := NewMyLogger()
	logger.Sync()

	var argu string
	for _, v := range os.Args {
		if strings.Index(v, "-mma") == 0 {
			argu = strings.Replace(v, "-mma=", "", -1)
			break
		}
	}
	MMAddress := argu
	if argu == "" {
		logger.Fatal("there is no Message Middleware address")
		return
	}

	args := new(Args)
	reply := new(Reply)

	currentIP := getCurrentIPAddr()
	const debugPK = 12345
	privateKey := float64(generateRanUint64())
	//privateKey := int64(generateRanUint64())
	shareKey := float64(9999)
	//shareKey := int64(9999)

	microService := MicroService{currentIP, privateKey, shareKey, make(chan bool), MMAddress}
	//microService := MicroService{currentIP, privateKey, shareKey, make(chan bool), MMAddress}
	receiver := Receiver{thisNodeAddr: currentIP, microService: microService}

	logger.Info("current machine address : " + currentIP + " / MM address : " + MMAddress)
	logger.Debug("private key : " + strconv.FormatUint(uint64(privateKey), 10))

	errReg := rpc.Register(receiver)
	if errReg != nil {
		log.Println(errReg)
		return
	}

	client, err := rpc.Dial("tcp", microService.MMAddress+":8160") // RPC 서버에 연결
	if err != nil {
		logger.Fatal(err.Error())
		return
	}
	defer client.Close() // main 함수가 끝나기 직전에 RPC 연결을 닫음

	go Listen()

	// Register 메세지 생성
	message := Message{From: microService.ClientAddr, Version: "1", Time: "1", Kind: RM}
	regMsg := RegisterImgMsg{message, microService.PrivateKey}
	RJsonMsg, _ := regMsg.ConvertToJson()
	args.JsonMsg = RJsonMsg
	args.Kind = RM
	// arguments로 MMReceive호출해서 MM으로 보냄
	err = client.Call("Receiver.MmReceive", args, reply)
	logger.Info("RM sent")
	if err != nil {
		fmt.Println(err)
		return
	}
	logger.Debug("[MM] " + reply.CompleteLog)

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		withDraw(message, client)
		//fmt.Println(sig)
		_ = sig
		done <- true
		fmt.Println("\nquit Moscato Successful and terminate microservice")
		os.Exit(0)
	}()

	// 연결 되면 채널로 연결 확인이 되어야 다음 단계로 넘어감
	checkConnect := <-microService.IsConnected
	_ = checkConnect
	// 엔터쳐야 다음으로 진행
	fmt.Scanln()
	logger.Info("sending Subscription messages")

	/*
		파일에서 subscription 읽어서 subscription 보내기
		 **/
	subFile, err := os.Open("subscription.txt")
	if err != nil {
		fmt.Println("sub")
		log.Fatalf("Error when opening file: %s", err)
	}
	defer subFile.Close()
	fileScanner := bufio.NewScanner(subFile)
	var subscriptionText string
	numSub := 0
	file1, _ := os.Create("./subTimeLog.log")
	defer file1.Close()
	for fileScanner.Scan() {
		subscriptionText = fileScanner.Text()
		//subscriptionTextSlice := strings.Split(subscriptionText, "/")
		//subTopic := subscriptionTextSlice[0]
		//subValue := subscriptionTextSlice[1]
		//subOperator := subscriptionTextSlice[2]
		//var subIsAlpha bool
		//if subscriptionTextSlice[3] == "true" {
		//	subIsAlpha = true
		//} else if subscriptionTextSlice[3] == "false" {
		//	subIsAlpha = false
		//} else {
		//	println(numSub, "subscription isAlpha type error.")
		//}
		fmt.Println(subscriptionText)
		numSub++
		message = Message{From: microService.ClientAddr, Version: "1", Time: "1", Kind: SM}

		// 메세지 생성시간 측정
		subMakingStartTime := time.Now()

		subMsg := CreateSubImage(message, subscriptionText)
		oriMsg := CreateSubImage(message, subscriptionText)
		//logger.Debug("subMsg #" + strconv.Itoa(numSub) + " topic: " + subTopic + " / value: " + subValue + " / operator: " + subOperator)
		logger.Debug("before enc subMsg #" + strconv.Itoa(numSub) + " topic: " + FloatSlice2String(subMsg.Topic))

		subMsg = EncryptionSubImage(subMsg, microService.ShareKey, microService.PrivateKey)

		OriginMsg = oriMsg

		logger.Debug("after enc subMsg #" + strconv.Itoa(numSub) + " topic: " + FloatSlice2String(subMsg.Topic))
		jsonMsg, _ := subMsg.ConvertToJson()
		args.JsonMsg = jsonMsg
		args.Kind = subMsg.Kind
		//fmt.Println(subMsg)

		//fmt.Println(string(args.JsonMsg))
		subMakingElapsedTime := time.Since(subMakingStartTime)

		// 메세지 생성 완료시간 측정
		fmt.Printf("sub 생성시간: %d\n", subMakingElapsedTime.Nanoseconds())

		fmt.Fprintln(file1, subMakingElapsedTime.Nanoseconds())

		err = client.Call("Receiver.MmReceive", args, reply)
		logger.Info("SM sent! #" + strconv.Itoa(numSub))

		if err != nil {
			fmt.Println(err)
			return
		}
		//log.Println(reply.CompleteLog)
		logger.Debug("[MM] " + reply.CompleteLog)

	}

	fmt.Scanln()
	logger.Info("sending Publish messages")

	pubFile, err := os.Open("publish.txt")
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}
	defer pubFile.Close()
	fileScanner2 := bufio.NewScanner(pubFile)
	var publishText string
	numPub := 0
	file2, _ := os.Create("./pubTimeLog.log")
	defer file2.Close()

	for fileScanner2.Scan() {
		publishText = fileScanner2.Text()
		//publishTextSlice := strings.Split(publishText, " ")
		//fmt.Println(publishTextSlice)
		pubTopic := publishText
		//pubValue := publishTextSlice[1]
		//pubContent := publishTextSlice[2]

		numPub++
		message = Message{From: microService.ClientAddr, Version: "1", Time: "1", Kind: PM}
		pubMakingStartTime := time.Now()

		pubMsg := CreatePubImage(message, pubTopic)
		logger.Debug("pubMsg #" + strconv.Itoa(numPub) + " topic: " + pubTopic)
		logger.Debug("before enc pubMsg #" + strconv.Itoa(numPub) + " topic: " + FloatSlice2String(pubMsg.Topic))

		// pubMsg Encryption
		fmt.Println("Publisher Private Key :", strconv.FormatUint(uint64(microService.PrivateKey), 10))
		pubMsg = EncryptionPubImage(pubMsg, microService.ShareKey, microService.PrivateKey)
		logger.Debug("after enc pubMsg #" + strconv.Itoa(numPub) + " topic: " + FloatSlice2String(pubMsg.Topic))

		jsonMsg, _ := pubMsg.ConvertToJson()
		fmt.Println(jsonMsg)
		args.JsonMsg = jsonMsg
		args.Kind = pubMsg.Kind

		pubMakingElapsedTime := time.Since(pubMakingStartTime)

		fmt.Printf("pub 생성시간: %d\n\n", pubMakingElapsedTime.Nanoseconds())
		fmt.Fprintln(file2, pubMakingElapsedTime.Nanoseconds())

		err = client.Call("Receiver.MmReceive", args, reply)
		logger.Info("PM sent! #" + strconv.Itoa(numPub))
		if err != nil {
			fmt.Println(err)
			return
		}

		logger.Debug("[MM]: " + reply.CompleteLog)

	}

	<-done
	return
}

func withDraw(msg Message, client *rpc.Client) {
	msg.Kind = WM
	wdMsg := WithdrawMsg{msg}
	var args Args
	args.JsonMsg, _ = wdMsg.ConvertToJson()
	args.Kind = WM
	var reply Reply
	client.Call("Receiver.MmReceive", args, reply)
}

func EncryptionPubImage(msg PublishImage, gyKey float64, privateKey float64) PublishImage {
	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] + (msg.Topic[index] * gyKey) + (msg.Topic[index] * privateKey)
	}
	return msg
}

func EncryptionPubMsg(msg PublishMsg, gyKey int64, privateKey int64) PublishMsg {
	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] + gyKey + privateKey
	}
	for index := range msg.Value {
		msg.Value[index] = msg.Value[index] + gyKey + privateKey
	}
	for index := range msg.Content {
		msg.Content[index] = msg.Content[index] + gyKey + privateKey
	}

	return msg
}

func EncryptionSubImage(msg SubscriptionImage, gyKey float64, privateKey float64) SubscriptionImage {
	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] + (msg.Topic[index] * gyKey) + (msg.Topic[index] * privateKey)
	}
	return msg
}

func EncryptionSubMsg(msg SubscriptionMsg, gyKey int64, privateKey int64) SubscriptionMsg {
	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] + gyKey + privateKey
	}
	for index := range msg.Value {
		msg.Value[index] = msg.Value[index] + gyKey + privateKey
	}
	return msg
}

func DecryptionImg(msg PublishImage, origin SubscriptionImage, gyKey float64, privateKey float64) {

	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] - (origin.Topic[index] * gyKey) - (origin.Topic[index] * privateKey)
	}

	var runeArr []rune
	for index := range msg.Topic {
		runeArr = append(runeArr, rune(int(msg.Topic[index])))
	}
	//fmt.Println("Topic is: " + string(runeArr))
	runeArr = nil

}

func DecryptionMsg(msg PublishMsg, gyKey int64, privateKey int64) {
	for index := range msg.Topic {
		msg.Topic[index] = msg.Topic[index] - gyKey - privateKey
	}
	for index := range msg.Value {
		msg.Value[index] = msg.Value[index] - gyKey - privateKey
	}
	for index := range msg.Content {
		msg.Content[index] = msg.Content[index] - gyKey - privateKey
	}

	var runeArr []rune
	for index := range msg.Topic {
		runeArr = append(runeArr, rune(int(msg.Topic[index])))
	}
	fmt.Println("Topic is: " + string(runeArr))
	runeArr = nil

	for index := range msg.Value {
		runeArr = append(runeArr, rune(int(msg.Value[index])))
	}

	if len(runeArr) == 1 {
		fmt.Println("Value is: ", runeArr)
	} else {
		fmt.Println("Value is:", string(runeArr))
	}

	runeArr = nil

	for index := range msg.Content {
		runeArr = append(runeArr, rune(int(msg.Content[index])))
	}
	fmt.Println("Content is: " + string(runeArr))
	runeArr = nil

}

func Listen() {
	l, err1 := net.Listen("tcp", ":8150") //MM로 부터 받는거
	if err1 != nil {
		log.Fatal(fmt.Sprintf("Unable to listen on given port: %s", err1))
	}
	defer l.Close()

	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}
func FloatSlice2String(target []float64) string {
	var targetString string
	targetString = ""
	for _, value := range target {
		targetString += strconv.FormatFloat(float64(value), 'f', -1, 64) + " "
	}
	return targetString
}

func IntSlice2String(target []int64) string {
	var targetString string
	targetString = ""
	for _, value := range target {
		targetString += strconv.FormatInt(int64(value), 10) + " "
	}
	return targetString
}
