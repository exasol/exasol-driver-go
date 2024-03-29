package wsconn

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WebsocketTestSuite struct {
	suite.Suite
}

func TestWebsocketSuite(t *testing.T) {
	suite.Run(t, new(WebsocketTestSuite))
}

func (suite *WebsocketTestSuite) TestVerifyPeerCertificate() {
	const errorMsgNoCertificate = "E-EGOD-9: server did not return certificates"
	const noFingerprint = ""
	for i, testCase := range []struct {
		certificate   [][]byte
		fingerprint   string
		expectedError string
	}{
		// Fingerprint configured
		{nil, "expectedFingerprint", errorMsgNoCertificate},
		{[][]byte{}, "expectedFingerprint", errorMsgNoCertificate},
		{[][]byte{[]byte("")}, "expectedFingerprint", "E-EGOD-10: the server's certificate fingerprint 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' does not match the expected fingerprint 'expectedFingerprint'"},
		{[][]byte{[]byte("certificateContent\n")}, "expectedFingerprint", "E-EGOD-10: the server's certificate fingerprint '77805314a4b617393d25bd7cf660963b4d41eee11381b1c5bab30db30710b416' does not match the expected fingerprint 'expectedFingerprint'"},
		{[][]byte{[]byte("certificateContent\n")}, "77805314a4b617393d25bd7cf660963b4d41eee11381b1c5bab30db30710b416", ""},
		{[][]byte{[]byte("certificateContent\n")}, "77805314A4B617393D25BD7CF660963B4D41EEE11381B1C5BAB30DB30710B416", ""},
		// No fingerprint configured
		{nil, noFingerprint, ""},
		{[][]byte{}, noFingerprint, ""},
		{[][]byte{[]byte("")}, noFingerprint, ""},
		{[][]byte{[]byte("certificateContent\n")}, noFingerprint, ""},
	} {
		suite.Run(fmt.Sprintf("Test %v: rawCerts=%q expectedFingerprint=%q", i, testCase.certificate, testCase.fingerprint), func() {
			verifier := certificateVerifier(testCase.fingerprint)
			err := verifier(testCase.certificate, nil)
			if testCase.expectedError == "" {
				suite.NoError(err)
			} else {
				suite.EqualError(err, testCase.expectedError)
			}
		})
	}
}

func (suite *WebsocketTestSuite) TestBytesToHexString() {
	for i, testCase := range []struct {
		data        []byte
		expectedHex string
	}{
		{nil, ""},
		{[]byte{}, ""},
		{[]byte{0}, "00"},
		{[]byte{1}, "01"},
		{[]byte{15}, "0f"},
		{[]byte{16}, "10"},
		{[]byte{255}, "ff"},
		{[]byte{0, 0}, "0000"},
		{[]byte{0, 1}, "0001"},
		{[]byte{255, 255}, "ffff"},
	} {
		suite.Run(fmt.Sprintf("Test %v: data=%q expectedHex=%q", i, testCase.data, testCase.expectedHex), func() {
			suite.Equal(testCase.expectedHex, bytesToHexString(testCase.data))
		})
	}
}
