package api

import (
	"github.com/dapr-platform/common"
	"github.com/go-chi/chi/v5"
	"net/http"
)

func InitTestRoute(r chi.Router) {

	r.Get(common.BASE_CONTEXT+"/", TestHandler)

}
func TestHandler(w http.ResponseWriter, r *http.Request) {
	//common.Logger.Debug(r.URL.RawQuery)
	//key  d, time, value
	//aai20580a022280d00022303101056382f00BS#aai20580a022280d00022303101056383400LS#aai20580a022280d00012303101056470d00FS#aai20580a022280d00082303101056481200BS#aai20580a022280d00082303101056481b00LS#aai20580a022280d00012303101057540000FS#aai20580a022280d00092303101057540400BS#aai20580a022280d00092303101057554200LS#10/03/23 10:59:52 - Logging Task Started#10/03/23 10:59:53 - System version v0.190#
	//10/03/23 10:59:53 - Hardware version v2#10/03/23 10:59:56 - SNTP: Automatically applying time correction of 0.01 seconds#10/03/23 11:03:48 - System version v0.190#10/03/23 11:03:48 - Hardware version v2#10/03/23 11:03:49 - System version v0.190#10/03/23 11:03:49 - Hardware version v2#10/03/23 11:03:49 - System version v0.190#
	val := r.URL.Query()
	for k, v := range val {
		common.Logger.Debug("key=", k)
		common.Logger.Debug("value=", v)
	}
	w.Write([]byte("success"))
}
