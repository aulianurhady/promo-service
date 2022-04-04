package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

var (
	DB     *sql.DB
	format string = "2006-01-02"
)

type Promotion struct {
	ID            int        `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	PromoCode     string     `json:"promo_code,omitempty" gorm:"type:varchar;unique"`
	PromoCodeType string     `json:"promo_code_type,omitempty" gorm:"type:ENUM('percentage','amount')"`
	Value         float64    `json:"value,omitempty" gorm:"type:numeric(16,2)"`
	StartDate     *time.Time `json:"start_date,omitempty" gorm:"type:timestamptz"`
	EndDate       *time.Time `json:"end_date,omitempty" gorm:"type:timestamptz"`
	Quota         int        `json:"quota,omitempty" gorm:"type:int"`
}

type PromotionByBookingDate struct {
	ID          int  `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	PromotionID int  `json:"promotion_id,omitempty" gorm:"type:int"`
	Sunday      bool `json:"sunday,omitempty" gorm:"type:bool"`
	Monday      bool `json:"monday,omitempty" gorm:"type:bool"`
	Tuesday     bool `json:"tuesday,omitempty" gorm:"type:bool"`
	Wednesday   bool `json:"wednesday,omitempty" gorm:"type:bool"`
	Thursday    bool `json:"thursday,omitempty" gorm:"type:bool"`
	Friday      bool `json:"friday,omitempty" gorm:"type:bool"`
	Saturday    bool `json:"saturday,omitempty" gorm:"type:bool"`
}

type PromotionByCheckinDate struct {
	ID          int  `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	PromotionID int  `json:"promotion_id,omitempty" gorm:"type:int"`
	Sunday      bool `json:"sunday,omitempty" gorm:"type:bool"`
	Monday      bool `json:"monday,omitempty" gorm:"type:bool"`
	Tuesday     bool `json:"tuesday,omitempty" gorm:"type:bool"`
	Wednesday   bool `json:"wednesday,omitempty" gorm:"type:bool"`
	Thursday    bool `json:"thursday,omitempty" gorm:"type:bool"`
	Friday      bool `json:"friday,omitempty" gorm:"type:bool"`
	Saturday    bool `json:"saturday,omitempty" gorm:"type:bool"`
}

type PromotionTransaction struct {
	ID            int `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	PromotionID   int `json:"promotion_id,omitempty" gorm:"type:int"`
	ReservationID int `json:"reservation_id,omitempty" gorm:"type:int"`
}

type DistributePromotion struct {
	ID                       int    `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	PromotionID              int    `json:"promotion_id,omitempty" gorm:"type:int"`
	PromotionByCheckinDateID int    `json:"promotion_by_checkin_date_id,omitempty" gorm:"type:int"`
	PromotionByBookingDateID int    `json:"promotion_by_booking_date_id,omitempty" gorm:"type:int"`
	StartHour                string `json:"start_hour,omitempty" gorm:"type:varchar"`
	EndHour                  string `json:"end_hour,omitempty,omitempty" gorm:"type:varchar"`
	QuotaInSunday            int    `json:"quota_in_sunday,omitempty" gorm:"type:int"`
	QuotaInMonday            int    `json:"quota_in_monday,omitempty" gorm:"type:int"`
	QuotaInTuesday           int    `json:"quota_in_tuesday,omitempty" gorm:"type:int"`
	QuotaInWednesday         int    `json:"quota_in_wednesday,omitempty" gorm:"type:int"`
	QuotaInThursday          int    `json:"quota_in_thurday,omitempty" gorm:"type:int"`
	QuotaInFriday            int    `json:"quota_in_friday,omitempty" gorm:"type:int"`
	QuotaInSaturday          int    `json:"quota_in_saturday,omitempty" gorm:"type:int"`
}

type AvailableRoom struct {
	RoomID     int    `json:"room_id,omitempty"`
	RoomNumber string `json:"room_number,omitempty"`
	Prices     Prices `json:"price,omitempty"`
}

type AvailableRooms []AvailableRoom

type Price struct {
	ID         int        `json:"id,omitempty" gorm:"primaryKey;unique;type:int"`
	Date       *time.Time `json:"date,omitempty" gorm:"type:timestampz"`
	RoomTypeID int        `json:"room_type_id,omitempty" gorm:"type:int"`
	Price      int        `json:"price,omitempty" gorm:"type:int"`
}

type Prices []Price

type Request struct {
	RoomQuantity int            `json:"room_qty"`
	RoomTypeID   int            `json:"room_type_id"`
	CheckinDate  string         `json:"checkin_date"`
	CheckoutDate string         `json:"checkout_date"`
	TotalPrice   int            `json:"total_price"`
	PromoID      int            `json:"promo_id"`
	List         AvailableRooms `json:"available_rooms"`
}

type ResponseData struct {
	RoomQuantity   int            `json:"room_qty"`
	RoomTypeID     int            `json:"room_type_id"`
	CheckinDate    string         `json:"checkin_date"`
	CheckoutDate   string         `json:"checkout_date"`
	TotalPrice     int            `json:"total_price"`
	AvailableRooms AvailableRooms `json:"available_rooms"`
}

type JsonResponse struct {
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data,omitempty"`
	Message    string      `json:"message,omitempty"`
}

func init() {
	GetConnection()
}

func GetConnection() error {
	host := "127.0.0.1"
	port := "5432"
	user := "postgres"
	password := "1234"
	dbname := "promotion_service"

	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	result, err := sql.Open("postgres", connString)
	if err != nil {
		log.Fatalf("Cannot connect to DB, cause %s", err)
		return err
	}

	err = result.Ping()
	if err != nil {
		log.Fatalf("Cannot ping db, cause %s", err)
		return err
	}

	DB = result

	return nil
}

func main() {
	http.HandleFunc("/redeem-promo", HandleRedeemPromo)

	fmt.Println("Listen to port 8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		panic("Cannot listen to port 8081")
	}
}

func HandleRedeemPromo(w http.ResponseWriter, r *http.Request) {
	res := ResponseData{}

	var request Request
	decoder := json.NewDecoder(r.Body)
	decoder.Decode(&request)

	str := fmt.Sprintf("%#v", request)
	fmt.Println("REQUEST: ", str)

	timeNow := time.Now()
	dayOfWeek := timeNow.Weekday()

	res.RoomQuantity = request.RoomQuantity
	res.RoomTypeID = request.RoomTypeID
	res.CheckinDate = request.CheckinDate
	res.CheckoutDate = request.CheckoutDate

	format := "2006-01-02"
	checkinDate, _ := time.Parse(format, request.CheckinDate)
	checkoutDate, _ := time.Parse(format, request.CheckoutDate)

	diff := checkoutDate.Sub(checkinDate)
	totalDiffDays := diff.Hours() / 24

	dataPromotion, _ := GetPromotion(request.PromoID)

	dataDistributePromo, _ := GetDistributePromo(request.PromoID)

	dataPromotionByBookingDate, _ := GetPromotionByBookingDate(request.PromoID)

	// Validate distribute promo
	isValidQuotaPromo := ValidatePromoByQuota(int(dayOfWeek), dataDistributePromo, dataPromotion, request, timeNow)
	if !isValidQuotaPromo {
		http.Error(w, errors.New("Data is not valid, promo cannot be use").Error(), http.StatusInternalServerError)
		return
	}

	// Validate requirement rules
	isValidRules, err := PromoRules(request, dataPromotion, dataPromotionByBookingDate, dataDistributePromo, int(dayOfWeek), int(totalDiffDays), timeNow)
	if !isValidRules {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	finalPrice, newListt := ListPriceWithPromo(request, dataPromotion.PromoCodeType, dataPromotion.Value)

	res.TotalPrice = finalPrice
	res.AvailableRooms = newListt

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func ListPriceWithPromo(req Request, promoType string, promoValue float64) (finalPrice int, newListWithPromo AvailableRooms) {
	for _, val1 := range req.List {
		newDataAvailableRoom := AvailableRoom{
			RoomID:     val1.RoomID,
			RoomNumber: val1.RoomNumber,
		}

		var newDataPrice Price
		for _, val2 := range val1.Prices {
			newPrice := CalculatePrice(val2.Price, promoType, promoValue)
			newDataPrice = Price{
				Date:  val2.Date,
				Price: newPrice,
			}

			newDataAvailableRoom.Prices = append(newDataAvailableRoom.Prices, newDataPrice)
		}

		newListWithPromo = append(newListWithPromo, newDataAvailableRoom)
	}

	finalPrice = CalculatePrice(req.TotalPrice, promoType, promoValue)

	return
}

func CalculatePrice(price int, promoType string, promoValue float64) int {
	if promoType == "percentage" {
		var discountAmount float64
		discountAmount = float64(price) * promoValue

		price = price - int(discountAmount)
	} else if promoType == "amount" {
		price = price - int(promoValue)
	}

	return price
}

func ValidatePromoByQuota(dayOfWeek int, dataDistributePromotion DistributePromotion, dataPromotion Promotion, req Request, currentTime time.Time) bool {
	totalPromoUsage, _ := CountTotalUsagePromo(req.PromoID)

	totalPromoInDayUsage, _ := CountTotalUsageInDay(req.PromoID, currentTime.Format(format))

	var quotaInDay int
	switch int(dayOfWeek) {
	case 0:
		// Sunday
		quotaInDay = dataDistributePromotion.QuotaInSunday
	case 1:
		// Monday
		quotaInDay = dataDistributePromotion.QuotaInMonday
	case 2:
		// Tuesday
		quotaInDay = dataDistributePromotion.QuotaInTuesday
	case 3:
		// Wednesday
		quotaInDay = dataDistributePromotion.QuotaInWednesday
	case 4:
		// Thursday
		quotaInDay = dataDistributePromotion.QuotaInThursday
	case 5:
		// Friday
		quotaInDay = dataDistributePromotion.QuotaInFriday
	case 6:
		// Saturday
		quotaInDay = dataDistributePromotion.QuotaInSaturday
	default:
		quotaInDay = 0
	}

	if quotaInDay <= totalPromoInDayUsage {
		return false
	}

	if dataPromotion.Quota <= totalPromoUsage {
		return false
	}

	return true
}

func PromoRules(req Request, dataPromotion Promotion, dataPromotionByBookingDate PromotionByBookingDate, dataDistributePromotion DistributePromotion, dayOfWeek, diffDays int, currentTime time.Time) (bool, error) {
	// minimum night
	if diffDays < 1 {
		return false, errors.New("Minimum night is not reached")
	}

	// minimum room
	if req.RoomQuantity < 1 {
		return false, errors.New("Minimum room is not reached")
	}

	// validate promo by checkin date
	startDate := dataPromotion.StartDate.Format(format)
	endDate := dataPromotion.EndDate.Format(format)

	if !ValidateCheckinDate(req.List, startDate, endDate) {
		return false, errors.New("Checkin date is not valid")
	}

	// validate promo by booking date
	if !ValidateBookingDate(dayOfWeek, dataPromotionByBookingDate) {
		return false, errors.New("Booking date is not valid")
	}

	// validate promo by booking hours
	if !validateHourTime(dataDistributePromotion, currentTime) {
		return false, errors.New("Hour time is not valid")
	}

	return true, nil
}

func validateHourTime(dataDistributePromotion DistributePromotion, currentTime time.Time) bool {
	hours := currentTime.Hour()
	startHour, _ := strconv.Atoi(dataDistributePromotion.StartHour)
	endHour, _ := strconv.Atoi(dataDistributePromotion.EndHour)

	fmt.Println("HOURS: ", hours)
	fmt.Println("START TIME: ", dataDistributePromotion.StartHour)
	fmt.Println("END TIME: ", dataDistributePromotion.EndHour)

	if !((hours >= startHour) && (hours < endHour)) {
		return false
	}

	return true
}

func GetPromotion(promoID int) (Promotion, error) {
	var promotion Promotion

	sql := `
		SELECT id, promo_code, promo_code_type, value, start_date, end_date, quota FROM promotion WHERE id = $1
	`
	statement, err := DB.Prepare(sql)
	if err != nil {
		log.Printf("Error query 13 %s", err)
		return promotion, err
	}

	err = statement.QueryRow(promoID).Scan(&promotion.ID, &promotion.PromoCode, &promotion.PromoCodeType, &promotion.Value, &promotion.StartDate, &promotion.EndDate, &promotion.Quota)
	if err != nil {
		log.Printf("Error query 12 %s", err)
		return promotion, err
	}

	return promotion, nil
}

func GetPromotionByBookingDate(promoID int) (PromotionByBookingDate, error) {
	var promotionByBookingDate PromotionByBookingDate

	sql := `
		SELECT id, promotion_id, sunday, monday, tuesday, wednesday, thursday, friday, saturday FROM promotion_by_booking_date WHERE promotion_id = $1
	`
	statement, err := DB.Prepare(sql)
	if err != nil {
		log.Printf("Error query 11 %s", err)
		return promotionByBookingDate, err
	}

	err = statement.QueryRow(promoID).Scan(&promotionByBookingDate.ID, &promotionByBookingDate.PromotionID, &promotionByBookingDate.Sunday, &promotionByBookingDate.Monday, &promotionByBookingDate.Tuesday, &promotionByBookingDate.Wednesday, &promotionByBookingDate.Thursday, &promotionByBookingDate.Friday, &promotionByBookingDate.Saturday)
	if err != nil {
		log.Printf("Error query 10 %s", err)
		return promotionByBookingDate, err
	}

	return promotionByBookingDate, nil
}

func CountTotalUsagePromo(promoID int) (int, error) {
	var (
		count_trx int = 0
	)

	sql := `
		SELECT COUNT(*) as count_trx FROM promotion_transaction WHERE promotion_id = $1
	`
	rows, err := DB.Query(sql, promoID)
	if err != nil {
		log.Printf("Error query 3 %s", err)
		return count_trx, err
	}

	for rows.Next() {
		rows.Scan(&count_trx)
	}

	return count_trx, nil
}

func CountTotalUsageInDay(promoID int, currentTime string) (int, error) {
	var (
		count_trx int = 0
	)

	modifyCurrentTimeStart := currentTime + " 00:00:00"
	modifyCurrentTimeEnd := currentTime + " 23:59:59"

	sql := `
		SELECT COUNT(*) as count_trx FROM promotion_transaction WHERE promotion_id = $1 AND (created_at >= $2 AND created_at <= $3)
	`
	rows, err := DB.Query(sql, promoID, modifyCurrentTimeStart, modifyCurrentTimeEnd)
	if err != nil {
		log.Printf("Error query 1 %s", err)
		return count_trx, err
	}

	for rows.Next() {
		rows.Scan(&count_trx)
	}

	return count_trx, nil
}

func GetDistributePromo(promoID int) (DistributePromotion, error) {
	var distributePromo DistributePromotion
	sql := `
		SELECT id, promotion_by_checkin_date_id, promotion_by_booking_date_id, start_hour, end_hour, quota_in_sunday, quota_in_monday, quota_in_tuesday, quota_in_wednesday, quota_in_thursday, quota_in_friday, quota_in_saturday FROM distribute_promotion WHERE id = $1
	`
	statement, err := DB.Prepare(sql)
	if err != nil {
		log.Printf("Error query 2 %s", err)
		return distributePromo, err
	}

	err = statement.QueryRow(promoID).Scan(&distributePromo.ID, &distributePromo.PromotionByCheckinDateID, &distributePromo.PromotionByBookingDateID, &distributePromo.StartHour, &distributePromo.EndHour, &distributePromo.QuotaInSunday, &distributePromo.QuotaInMonday, &distributePromo.QuotaInTuesday, &distributePromo.QuotaInWednesday, &distributePromo.QuotaInThursday, &distributePromo.QuotaInFriday, &distributePromo.QuotaInSaturday)
	if err != nil {
		log.Printf("Error query 3 %s", err)
		return distributePromo, err
	}

	return distributePromo, nil
}

func ValidateCheckinDate(list AvailableRooms, startDate, endDate string) bool {
	if len(list) <= 0 {
		return false
	}

	for _, val1 := range list {
		for _, val2 := range val1.Prices {
			if (val2.Date.String() >= startDate) && (val2.Date.String() <= endDate) {
				return true
			}
		}
	}

	return false
}

func ValidateBookingDate(currentDayOfWeek int, dataPromotionBookingDate PromotionByBookingDate) bool {
	switch currentDayOfWeek {
	case 0:
		// Sunday
		return dataPromotionBookingDate.Sunday
	case 1:
		// Monday
		return dataPromotionBookingDate.Monday
	case 2:
		// Tuesday
		return dataPromotionBookingDate.Tuesday
	case 3:
		// Wednesday
		return dataPromotionBookingDate.Wednesday
	case 4:
		// Thursday
		return dataPromotionBookingDate.Thursday
	case 5:
		// Friday
		return dataPromotionBookingDate.Friday
	case 6:
		// Saturday
		return dataPromotionBookingDate.Saturday
	}

	return false
}
