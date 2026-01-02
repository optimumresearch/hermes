package eth

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
    "bufio"
    "strings"
    "os"
    "errors"
//	pb "pb/telemetry"

	pb "github.com/probe-lab/hermes/eth/pb/telemetry"
	"google.golang.org/grpc"
)


func createData(dataChan chan<- *pb.LogEntry, name string) {
	// 3. Loop every 12 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	blknum := int64(100000)
	slot := int64(200000)
	blocksize := int64(34000)
	for range ticker.C {
		logEntry := &pb.LogEntry{
			Blocknumber: blknum,
			Slot:        slot,
			Blockhash:   fmt.Sprintf("%x", blknum),
			Timestamp:   time.Now().Unix(),
			Blocksize:   blocksize,
			ClientName:  name,
		}
		//Timestamp: time.Now().Unix(),
		blknum = blknum + 1
		slot = slot + 1
		blocksize = blocksize + 1

                dataChan <- logEntry  

		log.Println(name, "Data pushed to central server")
	}
}

func sendData(dataChan <-chan *pb.LogEntry, serverIP string) {
	for {
		streamError := false
		// 1. Connect to the central server
		conn, err := grpc.Dial(serverIP + ":50051", grpc.WithInsecure())
		if err != nil {
			log.Warnf("could not open connection: %v", err)
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			continue
		}

		defer conn.Close()
		client := pb.NewTelemetryServiceClient(conn)

		// 2. Open the stream
		stream, err := client.SendLogs(context.Background())
		if err != nil {
			log.Warnf("could not open stream: %v", err)
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			continue
		}

		// 3. Loop every 12 seconds
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for logentry :=  range dataChan {
			if err := stream.Send(logentry); err != nil {
				log.Printf("failed to send: %v", err)
				streamError = true
				break // Handle reconnection logic here
			}
			log.Println(logentry.ClientName, "Data pushed to central server")
		}

		if streamError {
			time.Sleep(2 * time.Second) // small delay before moving on
		}
	}
}


// GetFirstLine reads the first line from a file and returns it trimmed.
// Returns an error if the file doesn't exist or can't be read.
func getServerIP(filename string) (string, error) {
    // Check if file exists
    if _, err := os.Stat(filename); err != nil {
        if os.IsNotExist(err) {
            return "", errors.New("file does not exist")
        }
        return "", err
    }

    // Open the file
    file, err := os.Open(filename)
    if err != nil {
        return "", err
    }
    defer file.Close()

    // Read the first line
    scanner := bufio.NewScanner(file)
    if scanner.Scan() {
        return strings.TrimSpace(scanner.Text()), nil
    }

    // File is empty
    if err := scanner.Err(); err != nil {
        return "", err
    }
    return "", errors.New("file is empty")
}
