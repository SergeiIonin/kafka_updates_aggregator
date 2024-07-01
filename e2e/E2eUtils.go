package e2e

type MessageWithTopic interface {
	Topic() string
	/*MarshallJSON() ([]byte, error)
	UnmarshallJSON(data []byte) error*/
}

type MessageWithId struct {
	IdKey   string
	IdValue string
	Message MessageWithTopic
}

type Deposit struct {
	topic           string
	Balance         int    `json:"balance"`
	Deposit         int    `json:"deposit"`
	IsAuthenticated bool   `json:"isAuthenticated"`
	Country         string `json:"country"`
}

func (d Deposit) Topic() string {
	return d.topic
}
func NewDeposit(balance int, deposit int, isAuthenticated bool, country string) Deposit {
	return Deposit{topic: "user_deposit", Balance: balance, Deposit: deposit, IsAuthenticated: isAuthenticated, Country: country}
}

type Login struct {
	topic      string
	Login_time string `json:"login_time"`
}

func (d Login) Topic() string {
	return d.topic
}
func NewLogin(login_time string) Login {
	return Login{topic: "user_login", Login_time: login_time}
}

type Withdrawal struct {
	topic      string
	Withdrawal int `json:"withdrawal"`
}

func (d Withdrawal) Topic() string {
	return d.topic
}
func NewWithdrawal(withdrawal int) Withdrawal {
	return Withdrawal{topic: "user_withdrawal", Withdrawal: withdrawal}
}

type BalanceUpdates struct {
	topic      string
	Balance    string `json:"balance"`    // fixme: should be int
	Deposit    string `json:"deposit"`    // fixme: should be int
	Withdrawal string `json:"withdrawal"` // fixme: should be int
}

func (d BalanceUpdates) Topic() string {
	return d.topic
}
func NewBalanceUpdates(balance string, deposit string, withdrawal string) BalanceUpdates {
	return BalanceUpdates{topic: "", Balance: balance, Deposit: deposit, Withdrawal: withdrawal}
}

type LoginInfo struct {
	topic     string
	LoginTime string `json:"login_time"`
	Balance   string `json:"balance"` // fixme: should be int
}

func (d LoginInfo) Topic() string {
	return d.topic
}
func NewLoginInfo(loginTime string, balance string) LoginInfo {
	return LoginInfo{topic: "", LoginTime: loginTime, Balance: balance}
}
