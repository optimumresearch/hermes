package eth

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"time"

	pb "github.com/probe-lab/hermes/eth/pb/telemetry"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	eth "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/gofiber/fiber/v2/log"
	"google.golang.org/protobuf/proto"
)

// P2PMessage represents a message structure used in P2P communication
type P2PMessage struct {
	MessageID    string // Unique identifier for the message
	Topic        string // Topic name where the message was published
	Message      []byte // Actual message data
	SourceNodeID string // ID of the node that sent the message (we don't need it in future, it is just for debug purposes)
}

func writeToFile(ctx context.Context, dataCh <-chan string, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf(fmt.Sprintf("Cannot open file %s", filename))
		}
	}()

	writer := bufio.NewWriter(file)
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Error(fmt.Sprintf("Cannot open the writer %s", filename))
		}
	}()

	// Process until channel is closed
	for data := range dataCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if _, err = writer.WriteString(data + "\n"); err != nil {
			log.Errorf("Failed to write the data to %s", filename)
		}

		if err := writer.Flush(); err != nil {
			log.Errorf("Failed to flush the data to %s", filename)
		}
	}
	fmt.Println("All data flushed to disk")
}

// gets the basic block information from the proto.Message that comes from the Gossip
// Parameters:
//   - msg: the gossip message for the beacon block
//
// Returns:
//   - string with basic information on the block
//   - err
func getBeaconBlockInfo(msg proto.Message) (string, error) {
	signed, err := blocks.NewSignedBeaconBlock(msg)
	if err != nil {
		return "", err
	}
	if err := blocks.BeaconBlockIsNil(signed); err != nil {
		return "", err
	}

	block := signed.Block()
	body := block.Body()

	// Try to get execution payload info
	blockHash := ""
	blockNumber := uint64(0)
	blockSize := int(0)

	if executionPayload, err := body.Execution(); err == nil {
		blockSize = len(executionPayload.ExtraData()) + 32*5 // Basic size estimation
		blockHash = fmt.Sprintf("%#x", executionPayload.BlockHash())
		blockNumber = executionPayload.BlockNumber()

		txns, _ := executionPayload.Transactions()
		for _, tx := range txns {
			blockSize += len(tx)
		}
	}

	timeUnix := time.Now().UnixMilli()
	blockInfoString := fmt.Sprintf("%d\t%d\t%s\t%d\t%d", blockNumber, block.Slot(), blockHash, timeUnix, blockSize)

	return blockInfoString, nil
}

func unmarshalMumP2PMessageToBlock(binary_data []byte) (*eth.SignedBeaconBlockFulu, error) {
	newBlock := &eth.SignedBeaconBlockFulu{}
	if err := proto.Unmarshal(binary_data, newBlock); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal")
	}
	return newBlock, nil
}

// gets the basic block information from eth.SignedBeaconBlockFulu, the that comes from the Gossip
// Parameters:
//   - msg: the gossip message for the beacon block
//
// Returns:
//   - string with basic information on the block
//   - err
func getBeaconBlockInfo1(fuluBlock *eth.SignedBeaconBlockFulu) (string, error) {
	// 2. Access the Slot number, which is nested within the block's Message.
	slotNumber := fuluBlock.GetBlock().Slot
	blockNumber := fuluBlock.GetBlock().Body.ExecutionPayload.BlockNumber
	blockHash := fuluBlock.GetBlock().Body.ExecutionPayload.BlockHash
	blockSize := fuluBlock.GetBlock().Body.SizeSSZ()
	timeUnix := time.Now().UnixMilli()

	blockInfoString := fmt.Sprintf("%d\t%d\t%#x\t%d\t%d", blockNumber, slotNumber, blockHash, timeUnix, blockSize)

	return blockInfoString, nil

}

func getBeaconBlockInfo2(fuluBlock *eth.SignedBeaconBlockFulu, hostID string) (*pb.LogEntry, error) {
	// 2. Access the Slot number, which is nested within the block's Message.
	slotNumber := fuluBlock.GetBlock().Slot
	blockNumber := fuluBlock.GetBlock().Body.ExecutionPayload.BlockNumber
	blockHash := fuluBlock.GetBlock().Body.ExecutionPayload.BlockHash
	blockSize := fuluBlock.GetBlock().Body.SizeSSZ()
	timeUnix := time.Now().UnixMilli()

	logEntry := &pb.LogEntry{
		Blocknumber: int64(blockNumber),
		Slot:        int64(slotNumber),
		Blockhash:   fmt.Sprintf("%#x", blockHash),
		Timestamp:   timeUnix,
		Blocksize:   int64(blockSize),
		ClientName:  hostID,
	}
        return logEntry,  nil
}
