package imgtask

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/png"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	imgdatabasemodels "github.com/KanybekMomukeyev/imgdatabase/v3/models"
	dbmodels "github.com/KanybekMomukeyev/imgdatabase/v3/models/dbmodels"
	"github.com/hibiken/asynq"
)

// -------------------------------------------------- //
const (
	TypeWelcomeEmail  = "email:welcome"
	TypeReminderEmail = "email:reminder"
	// TIMEOUTOPAYMENT
	TIMEOUTOPAYMENT = 200
)

// -------------------------------------------------- //
func MakeTimestamp() int64 {
	time.Sleep(time.Millisecond)
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// -------------------------------------------------- //
func UnmarshalImageResp(data []byte) (ImageResp, error) {
	var r ImageResp
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *ImageResp) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type ImageResp struct {
	Result []string `json:"result"`
}

// -------------------------------------------------- //

// Task payload for any email related tasks.
type emailTaskPayload struct {
	ImageScanPath string
	DocModelID    uint64
	DocName       string
}

// --------------------------------------------------------------------------------------- //
func NewWelcomeEmailTask(docModelID uint64, imageScanPath string, docName string) (*asynq.Task, error) {
	payload, err := json.Marshal(emailTaskPayload{DocModelID: docModelID, ImageScanPath: imageScanPath, DocName: docName})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeWelcomeEmail, payload), nil
}

// This one used only for queue server, only queue server!
func HandleWelcomeEmailTask(ctx context.Context, t *asynq.Task) error {
	var taskPayload emailTaskPayload
	if err := json.Unmarshal(t.Payload(), &taskPayload); err != nil {
		return err
	}
	log.Printf(" [*] START WELCOME CUDA SERVER ID=%d, ScanPath=%s, DocName=%s", taskPayload.DocModelID, taskPayload.ImageScanPath, taskPayload.DocName)
	time.Sleep(10 * time.Second)
	uploadToPythonCudaServerAsync(taskPayload.ImageScanPath, taskPayload.DocModelID, taskPayload.DocName)
	log.Printf(" [*] END WELCOME CUDA SERVER ID=%%d", taskPayload.DocModelID)
	return nil
}

// --------------------------------------------------------------------------------------- //
func NewReminderEmailTask(docModelID uint64, imageScanPath string, docName string) (*asynq.Task, error) {
	payload, err := json.Marshal(emailTaskPayload{DocModelID: docModelID, ImageScanPath: imageScanPath, DocName: docName})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeReminderEmail, payload), nil
}

func HandleReminderEmailTask(ctx context.Context, t *asynq.Task) error {
	var taskPayload emailTaskPayload
	if err := json.Unmarshal(t.Payload(), &taskPayload); err != nil {
		return err
	}
	log.Printf(" [*] START Send REMINDER Email to User %d", taskPayload.DocModelID)
	time.Sleep(10 * time.Second)
	log.Printf(" [*] END Send REMINDER Email to User %d", taskPayload.DocModelID)
	return nil
}

// --------------------------------------------------------------------------------------- //
const (
	// LOGSPATH
	LOGSPATH = "cachedblog/worker_uploader_service.log"

	// PARSED_IMAGE
	PARSED_IMAGE = "file_store/"
)

func uploadToPythonCudaServerAsync(imagePath string, docModelID uint64, docName string) error {

	var providerLogs = log.New()
	providerLogs.SetFormatter(&log.JSONFormatter{})
	providerLogs.SetOutput(os.Stdout)
	providerLogs.SetLevel(log.InfoLevel)
	providerFile, err := os.OpenFile(LOGSPATH, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		providerLogs.Out = providerFile
	} else {
		fmt.Printf("\nLOG OPEN CRASH")
		providerLogs.Fatal(err)
	}
	defer providerFile.Close()

	host := "database"
	port := 5432

	connectionInfo := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		host,
		port,
		os.Getenv("POSTGRES_DB"))

	providerLogs.WithFields(log.Fields{"connectionInfo": connectionInfo}).Info("PROD_PostgreConnection")
	fmt.Printf("\n\n connectionInfo: %s \n\n", connectionInfo)
	dbMng := imgdatabasemodels.NewDbManager(connectionInfo, providerLogs)

	dbMng.TestSqlxDatabaseConnection()
	dbMng.DropAllTables()

	//-----------
	// Read file
	//-----------
	originalImageSrc, err := os.Open(imagePath)
	if err != nil {
		return err
	}
	defer originalImageSrc.Close()

	//-----------
	// Create folder
	//-----------
	uudiddFolderName := uuid.NewV4().String()

	folderPath := filepath.Join("./file_store", uudiddFolderName)
	err = os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return err
	}

	//-----------
	// parsed_images create folder
	//-----------
	parsed_images_path := "./file_store/" + uudiddFolderName
	parsed_images_folder_path := filepath.Join(parsed_images_path, "parsed_images")
	err = os.MkdirAll(parsed_images_folder_path, os.ModePerm)
	if err != nil {
		return err
	}

	//-----------
	// Create folder temporary file
	//-----------
	formattedTimeFileName := time.Now().Format("2006-01-02_Mon_15:04:05")
	finalImagePath := fmt.Sprintf("%s%s%s%s%s",
		"file_store/",
		uudiddFolderName,
		"/",
		formattedTimeFileName,
		"_parsed.jpg",
	)

	destOfImage, err := os.Create(finalImagePath)
	if err != nil {
		return err
	}
	defer destOfImage.Close()

	//-----------
	// Copy image to folder image
	//-----------
	if _, err := io.Copy(destOfImage, originalImageSrc); err != nil {
		return err
	}

	// ------ CREATE DB FOLDER MODEL ------ //
	folderModel := new(dbmodels.FolderModel)
	folderModel.WordCount = uint64(54321)
	folderModel.DocmodelID = docModelID
	folderModel.CompanyID = uint64(1)
	folderModel.FolderImagePath = finalImagePath
	folderModel.FolderName = uudiddFolderName
	folderModel.ContactFname = docName
	folderModel.PhoneNumber = parsed_images_folder_path
	folderModel.Address = folderPath
	folderModelId, err := dbMng.CreateFolderModel(folderModel, 3000)
	if err != nil {
		log.Fatal(err)
		return err
	}
	folderModel.FolderID = folderModelId
	cuttedImages := make([]*dbmodels.CuttedImage, 0)

	// ----------------------------------- //

	fmt.Printf("\n\n STARTING PARSE %s\n", finalImagePath)
	// --------- REQUEST IMAGE TO PYTHON GPU SERVER --------- //
	if true {
		// ------------------------ //
		file, err := os.Open(finalImagePath)
		if err != nil {
			log.Fatal(err)
			return err
		}
		defer file.Close()
		// ------------------------ //
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))

		if err != nil {
			log.Fatal(err)
			return err
		}

		io.Copy(part, file)
		writer.Close()

		request, err := http.NewRequest("POST", "http://104.248.130.106:5000/upload", body)
		// request, err := http.NewRequest("POST", "http://10.20.0.76:5000/upload", body)
		// request, err := http.NewRequest("POST", "http://127.0.0.1:5000/upload", body)

		if err != nil {
			log.Fatal(err)
			return err
		}

		request.Header.Add("Content-Type", writer.FormDataContentType())
		request.Header.Add("folderModelId", strconv.FormatUint(folderModelId, 10))

		timeout := time.Duration(TIMEOUTOPAYMENT * time.Second)
		client := &http.Client{Timeout: timeout}

		response, err := client.Do(request)

		if err != nil {
			log.Fatal(err)
			return err
		}
		defer response.Body.Close()

		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
			return err
		}

		// fmt.Printf("\n\n RESPONSE BODY: %s", string(content))
		fmt.Printf("\n\n RESPONSE STATUS: %s", response.Status)
		fmt.Printf("\n\n RESPONSE HEADER: %s", response.Header)

		imageRespInfo, err := UnmarshalImageResp(content)
		if err != nil {
			log.Fatal(err)
			return err
		}

		for _, imageRespInfo := range imageRespInfo.Result {
			unbased, err := base64.StdEncoding.DecodeString(imageRespInfo)
			if err != nil {
				panic("Cannot decode b64")
			}

			r := bytes.NewReader(unbased)
			im, err := png.Decode(r)
			if err != nil {
				panic("Bad png")
			}

			milliSeconds := MakeTimestamp()
			milliSecondsString := strconv.FormatInt(int64(milliSeconds), 10)
			milliSecondsString = parsed_images_folder_path + "/" + milliSecondsString + ".png"

			f, err := os.OpenFile(milliSecondsString, os.O_WRONLY|os.O_CREATE, 0777)
			if err != nil {
				panic("Cannot open file")
			}

			err = png.Encode(f, im)
			if err != nil {
				panic("Bad png")
			}

			cuttedimage := new(dbmodels.CuttedImage)
			cuttedimage.CuttedImageState = uint32(dbmodels.CUTTED_IMAGE_NOT_TRANSLATED) // not translated yet.
			cuttedimage.DocModelID = docModelID
			cuttedimage.CompanyID = uint64(1)
			cuttedimage.FolderID = folderModel.FolderID
			cuttedimage.ParsedImagePath = milliSecondsString
			cuttedimage.FolderName = folderModel.FolderName
			cuttedimage.SecondName = docName
			cuttedimage.PhoneNumber = docName
			cuttedimage.Address = docName

			cuttedimageId, err := dbMng.CreateCuttedImage(cuttedimage, cuttedimage.CompanyID)
			if err != nil {
				panic("CreateCuttedImage")
			}
			cuttedimage.CuttedImageID = cuttedimageId
			cuttedImages = append(cuttedImages, cuttedimage)
		}

		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}
