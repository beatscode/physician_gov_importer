package main

import (
	"encoding/csv"
	//"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	//"github.com/pkg/profile"
	_ "io"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

//Specialty
type Specialty struct {
	speID            string
	speName          string
	ForeignSpecialty string
}

func readCSV(path string) [][]string {
	filehandle, err := os.Open(path)
	checkErr(err)
	defer filehandle.Close()

	reader := csv.NewReader(filehandle)

	records, err := reader.ReadAll()
	checkErr(err)
	return records
}

//getSpecialtyIDFromPhysicianSpecialty will return the speID from
//contentlibrary based on physician specialty
func getSpecialtyIDFromPhysicianSpecialty(physicianSpecialty string) string {
	var specialtyID string
	for _, specialty := range specialtyArray {
		if specialty.ForeignSpecialty == physicianSpecialty {
			specialtyID = specialty.speID
			break
		}
	}
	return specialtyID
}

var physician Physician
var physicians []Physician
var specialtyArray []Specialty
var readLimit = 10000
var db *gorm.DB
var wg sync.WaitGroup
var specialties [][]string
var physicianSpecialties [][]string
var physicianCSV = "/Users/alex/Downloads/Physician_Compare_National_Downloadable_File.csv"
var err error

func buildSpecialtyArray() {
	//Build Mapped Specialty List
	for k, v := range specialties {
		if k == 0 {
			//skip header
			continue
		}
		for _, x := range physicianSpecialties {
			if v[1] == x[1] {
				specialtyArray = append(specialtyArray, Specialty{v[0], v[1], x[0]})
			}
		}
	}
}

func main() {
	//defer profile.Start().Stop()
	//	p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	specialtyCombinedCSV := "big_physician_specialty_list_combined.csv"
	specialtiesCSV := "specialties.csv"
	physicianSpecialties = readCSV(specialtyCombinedCSV)
	specialties = readCSV(specialtiesCSV)

	db, err = gorm.Open("mysql", "db_leoprod:4DMXexDaw8s@/data_gov?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		log.Fatal("Cannot open DB connection", err)
	}
	defer db.Close()
	db.DB().SetMaxOpenConns(0)
	db.AutoMigrate(&Physician{})
	db.Model(&Physician{}).AddIndex("idx_last_name_state", "last_name", "state")
	//fmt.Println(db.HasTable(&Physician{}))
	buildSpecialtyArray()
	//Open File
	//Read each line until EOF
	filehandle, err := os.Open(physicianCSV)
	checkErr(err)
	defer filehandle.Close()

	reader := csv.NewReader(filehandle)
	_, err = reader.Read()
	checkErr(err)

	var tx *gorm.DB
	tx = db.Begin()
	db.Exec("truncate physicians")
	for i := 0; i < readLimit; i++ {
		//Reads Next Record and unmarshals into physician type
		//err := Unmarshal(reader, &physician)
		//checkErr(err)
		record, err := reader.Read()
		if err != nil {
			panic(err)
		}
		physician = convertCSVRecordToPhysician(record)
		//loop through to find strings
		//replace string if
		physician.FirstName = strings.Title(physician.FirstName)
		physician.LastName = strings.Title(physician.LastName)
		if physician.MedicalSchoolName == "OTHER" {
			physician.MedicalSchoolName = ""
		} else {
			CapitalizeTitle(&physician.MedicalSchoolName)
		}

		physician.MiddleName = strings.Title(physician.MiddleName)
		CapitalizeTitle(&physician.OrganizationLegalName)
		CapitalizeTitle(&physician.Line1StreetAddress)
		CapitalizeTitle(&physician.Line2StreetAddress)
		CapitalizeTitle(&physician.City)
		physician.SpecialtyID = getSpecialtyIDFromPhysicianSpecialty(physician.PrimarySpecialty)
		wg.Add(1)
		go massageAndSavePhysician(physician, tx)
	}
	tx.Commit()
	fmt.Println(readLimit, "records inserted")
	wg.Wait()
	//p.Stop()
	//fmt.Println(len(physicians), "physicians")
}

func massageAndSavePhysician(physician Physician, tx *gorm.DB) {
	/*	db, err := gorm.Open("mysql", "db_leoprod:4DMXexDaw8s@/data_gov?charset=utf8&parseTime=True&loc=Local")
		if err != nil {
			log.Fatal("Cannot open DB connection", err)
		}
		defer db.Close()
	*/defer wg.Done()
	tx.Create(&physician)
	//db.Create(&physician)
}

//CapitalizeTitle set the proper title style
func CapitalizeTitle(title *string) {
	defer func() {
		if x := recover(); x != nil {
			log.Println("Error", x)
			log.Println("Title: ", *title, len(*title))
		}
	}()
	if len(*title) == 0 {
		return
	}
	*title = strings.Title(strings.ToLower(*title))

	prepositions := []string{"TO", "OR", "OF", "FOR", "AT", "AND", "IN", "BY", "THE"}
	var re *regexp.Regexp
	for _, preposition := range prepositions {
		if strings.Contains(*title, preposition) {
			if strings.LastIndex(*title, preposition) == 0 {
				*title = re.ReplaceAllString(*title, strings.ToLower(preposition))
				continue
			}
			matchStr := fmt.Sprintf(`\b%s\b`, strings.Title(strings.ToLower(preposition)))
			re = regexp.MustCompile(matchStr)
			*title = re.ReplaceAllString(*title, strings.ToLower(preposition))
			if strings.ToLower(preposition) == "the" {
				regex := regexp.MustCompile("^THE|^the")
				*title = regex.ReplaceAllString(*title, "The")
			}
		}
	}
	//t := *title
	//*title = fmt.Sprintf("%s%s", strings.ToUpper(t[0:1]), t[1:len(t)])
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func convertCSVRecordToPhysician(record []string) Physician {

	var phy Physician

	phy.NPI = record[0]
	phy.PACID = record[1]
	phy.ProfessionalEnrollmentID = record[2]
	phy.LastName = record[3]
	phy.FirstName = record[4]
	phy.MiddleName = record[5]
	phy.Suffix = record[6]
	phy.Gender = record[7]
	phy.Credential = record[8]
	phy.MedicalSchoolName = record[9]
	phy.GraduationYear = record[10]
	phy.PrimarySpecialty = record[11]
	phy.SecondarySpecialty1 = record[12]
	phy.SecondarySpecialty2 = record[13]
	phy.SecondarySpecialty3 = record[14]
	phy.SecondarySpecialty4 = record[15]
	phy.AllSecondarySpecialties = record[16]
	phy.OrganizationLegalName = record[17]
	phy.GroupPracticePACID = record[18]
	phy.NumberOfGroupPracticeMembers = record[19]
	phy.Line1StreetAddress = record[20]
	phy.Line2StreetAddress = record[21]
	phy.MarkerOfAddressLine2Suppression = record[22]
	phy.City = record[23]
	phy.State = record[24]
	phy.ZipCode = record[25]
	phy.PhoneNumber = record[26]
	phy.HospitalAffiliationCCN1 = record[27]
	phy.HospitalAffiliationLBN1 = record[28]
	phy.HospitalAffiliationCCN2 = record[29]
	phy.HospitalAffiliationLBN2 = record[30]
	phy.HospitalAffiliationCCN3 = record[31]
	phy.HospitalAffiliationLBN3 = record[32]
	phy.HospitalAffiliationCCN4 = record[33]
	phy.HospitalAffiliationLBN4 = record[34]
	phy.HospitalAffiliationCCN5 = record[35]
	phy.HospitalAffiliationLBN5 = record[36]
	phy.ProfessionalAcceptsMedicareAssignment = record[37]
	phy.ReportedQualityMeasures = record[38]
	phy.UsedElectronicHealthRecords = record[39]
	phy.ParticipatedInTheMedicareMaintenance = record[40]
	phy.CommittedToHeartHealth = record[41]
	return phy
}

func Unmarshal(reader *csv.Reader, v interface{}) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}
	s := reflect.ValueOf(v).Elem()
	/*if s.NumField() != len(record) {
		return &FieldMismatch{s.NumField(), len(record)}
	}*/
	for i := 0; i < s.NumField(); i++ {
		if i >= len(record) {
			break
		}
		f := s.Field(i)
		switch f.Type().String() {
		case "string":
			f.SetString(record[i])
		case "int":
			ival, err := strconv.ParseInt(record[i], 10, 0)
			if err != nil {
				return err
			}
			f.SetInt(ival)
		default:
			return &UnsupportedType{f.Type().String()}
		}
	}
	return nil
}

type Physician struct {
	NPI                                   string
	PACID                                 string `gorm:"column:pacid"`
	ProfessionalEnrollmentID              string
	LastName                              string
	FirstName                             string
	MiddleName                            string
	Suffix                                string
	Gender                                string
	Credential                            string
	MedicalSchoolName                     string
	GraduationYear                        string
	PrimarySpecialty                      string
	SecondarySpecialty1                   string
	SecondarySpecialty2                   string
	SecondarySpecialty3                   string
	SecondarySpecialty4                   string
	AllSecondarySpecialties               string
	OrganizationLegalName                 string
	GroupPracticePACID                    string
	NumberOfGroupPracticeMembers          string
	Line1StreetAddress                    string
	Line2StreetAddress                    string
	MarkerOfAddressLine2Suppression       string
	City                                  string
	State                                 string
	ZipCode                               string
	PhoneNumber                           string
	HospitalAffiliationCCN1               string
	HospitalAffiliationLBN1               string
	HospitalAffiliationCCN2               string
	HospitalAffiliationLBN2               string
	HospitalAffiliationCCN3               string
	HospitalAffiliationLBN3               string
	HospitalAffiliationCCN4               string
	HospitalAffiliationLBN4               string
	HospitalAffiliationCCN5               string
	HospitalAffiliationLBN5               string
	ProfessionalAcceptsMedicareAssignment string
	ReportedQualityMeasures               string
	UsedElectronicHealthRecords           string
	ParticipatedInTheMedicareMaintenance  string
	CommittedToHeartHealth                string
	SpecialtyID                           string
}

type FieldMismatch struct {
	expected, found int
}

func (e *FieldMismatch) Error() string {
	return "CSV line fields mismatch. Expected " + strconv.Itoa(e.expected) + " found " + strconv.Itoa(e.found)
}

type UnsupportedType struct {
	Type string
}

func (e *UnsupportedType) Error() string {
	return "Unsupported type: " + e.Type
}
