package main

import (
	"encoding/csv"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var physician Physician
var physicians []Physician
var specialtyArray []Specialty
var readLimit = 23000000
var bulkAmount = 1000
var db *gorm.DB
var wg sync.WaitGroup
var specialties [][]string
var physicianSpecialties [][]string
var physicianCSV = "Physician_Compare_National_Downloadable_File.csv"
var specialtyCombinedCSV = "big_physician_specialty_list_combined.csv"
var specialtiesCSV = "specialties.csv"
var err error

//Specialty from content library
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

func dbConnect() {

	db, err = gorm.Open("mysql", "db_leoprod:4DMXexDaw8s@/data_gov?charset=utf8&multiStatements=true&parseTime=True&loc=Local")
	if err != nil {
		log.Fatal("Cannot open DB connection", err)
	}

}
func main() {

	physicianSpecialties = readCSV(specialtyCombinedCSV)
	specialties = readCSV(specialtiesCSV)
	dbConnect()
	defer db.Close()
	db.DB().SetMaxOpenConns(99)
	db.DB().SetMaxIdleConns(50)
	db.AutoMigrate(&Physician{})
	db.Model(&Physician{}).AddIndex("idx_last_name_state", "last_name", "state")
	db.Model(&Physician{}).AddIndex("idx_unique_npi", "npi")
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

	db.Exec("truncate physicians")
	for i := 0; i <= readLimit; i++ {
		//Reads Next Record and unmarshals into physician type
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		physician = convertCSVRecordToPhysician(record)
		//loop through to find strings
		//replace string if
		physician.FirstName = strings.Title(strings.ToLower(physician.FirstName))
		physician.LastName = strings.Title(strings.ToLower(physician.LastName))
		if physician.MedicalSchoolName == "OTHER" {
			physician.MedicalSchoolName = ""
		} else {
			CapitalizeTitle(&physician.MedicalSchoolName)
		}

		physician.MiddleName = strings.Title(strings.ToLower(physician.MiddleName))
		CapitalizeTitle(&physician.OrganizationLegalName)
		CapitalizeTitle(&physician.Line1StreetAddress)
		CapitalizeTitle(&physician.Line2StreetAddress)
		CapitalizeTitle(&physician.City)
		physician.SpecialtyID = getSpecialtyIDFromPhysicianSpecialty(physician.PrimarySpecialty)

		physicians = append(physicians, physician)

		if math.Mod(float64(i), float64(bulkAmount)) == 0 && i != 0 {
			fmt.Println(i, "Records: From ", i-bulkAmount, "to", i)
			wg.Add(1)
			go bulkSavePhysicians(physicians[i-bulkAmount : i])
		}
	}

	fmt.Println(readLimit, "records inserted")
	wg.Wait()
}

func bulkSavePhysicians(_physicians []Physician) {
	defer func() {
		if x := recover(); x != nil {
			fmt.Println(x)
			//log.Fatal(x)
			time.Sleep(time.Duration(5) * time.Second)
		}

	}()
	defer wg.Done()

	sqlStringArray := buildSQLStatements(_physicians)
	batchSQL := fmt.Sprintf("insert ignore into physicians values %s ;", strings.Join(sqlStringArray, ","))
	tx := db.Begin()
	errors := tx.Exec(batchSQL).GetErrors()
	if len(errors) > 0 {
		panic(errors)
	}
	tx.Commit()
}

func buildSQLStatements(_physicians []Physician) []string {

	var valueStr string
	var valueArr []string
	for _, phys := range _physicians {

		valueStr = fmt.Sprintf(`( "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s" )`, phys.NPI,
			phys.PACID,
			phys.ProfessionalEnrollmentID,
			strings.Replace(phys.LastName, "'", "\\'", -1),
			phys.FirstName,
			phys.MiddleName,
			phys.Suffix,
			phys.Gender,
			phys.Credential,
			strings.Replace(phys.MedicalSchoolName, "'", "\\'", -1),
			phys.GraduationYear,
			phys.PrimarySpecialty,
			phys.SecondarySpecialty1,
			phys.SecondarySpecialty2,
			phys.SecondarySpecialty3,
			phys.SecondarySpecialty4,
			phys.AllSecondarySpecialties,
			strings.Replace(phys.OrganizationLegalName, "'", "\\'", -1),
			phys.GroupPracticePACID,
			phys.NumberOfGroupPracticeMembers,
			strings.Replace(phys.Line1StreetAddress, "'", "\\'", -1),
			phys.Line2StreetAddress,
			phys.MarkerOfAddressLine2Suppression,
			phys.City,
			phys.State,
			phys.ZipCode,
			phys.PhoneNumber,
			phys.HospitalAffiliationCCN1,
			phys.HospitalAffiliationLBN1,
			phys.HospitalAffiliationCCN2,
			phys.HospitalAffiliationLBN2,
			phys.HospitalAffiliationCCN3,
			phys.HospitalAffiliationLBN3,
			phys.HospitalAffiliationCCN4,
			phys.HospitalAffiliationLBN4,
			phys.HospitalAffiliationCCN5,
			phys.HospitalAffiliationLBN5,
			phys.ProfessionalAcceptsMedicareAssignment,
			phys.ReportedQualityMeasures,
			phys.UsedElectronicHealthRecords,
			phys.ParticipatedInTheMedicareMaintenance,
			phys.CommittedToHeartHealth,
			phys.SpecialtyID)

		valueArr = append(valueArr, valueStr)
	}
	return valueArr
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
