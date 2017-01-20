package main

import (
	"encoding/csv"
	//"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "io"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
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
var readLimit = 3000000

func main() {
	physicianCSV := "/Users/alex/Downloads/Physician_Compare_National_Downloadable_File.csv"
	specialtyCombinedCSV := "big_physician_specialty_list_combined.csv"
	specialtiesCSV := "specialties.csv"
	physicianSpecialties := readCSV(specialtyCombinedCSV)
	specialties := readCSV(specialtiesCSV)

	db, err := gorm.Open("mysql", "db_leoprod:4DMXexDaw8s@/data_gov?charset=utf8&parseTime=True&loc=Local")
	defer db.Close()

	db.AutoMigrate(&Physician{})
	db.Model(&Physician{}).AddIndex("idx_last_name_state", "last_name", "state")
	//fmt.Println(db.HasTable(&Physician{}))

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
	tx.Exec("truncate physicians")
	for i := 0; i < readLimit; i++ {
		massageAndSavePhysician(reader, tx)
	}
	tx.Commit()
	//	fmt.Println(len(physicians), "physicians")
}

func massageAndSavePhysician(reader *csv.Reader, tx *gorm.DB) {
	//Reads Next Record and unmarshals into physician type
	err := Unmarshal(reader, &physician)
	checkErr(err)

	//loop through to find strings
	//replace string if
	CapitalizeTitle(&physician.FirstName)
	CapitalizeTitle(&physician.LastName)
	if physician.MedicalSchoolName == "OTHER" {
		physician.MedicalSchoolName = ""
	} else {
		CapitalizeTitle(&physician.MedicalSchoolName)
	}

	CapitalizeTitle(&physician.MiddleName)
	CapitalizeTitle(&physician.OrganizationLegalName)
	CapitalizeTitle(&physician.Line1StreetAddress)
	CapitalizeTitle(&physician.Line2StreetAddress)
	CapitalizeTitle(&physician.City)
	physician.specialtyID = getSpecialtyIDFromPhysicianSpecialty(physician.PrimarySpecialty)
	tx.Create(&physician)
	//physicians = append(physicians, physician)
}

//CapitalizeTitle set the proper title style
func CapitalizeTitle(title *string) {
	if len(*title) == 0 {
		return
	}
	*title = strings.Title(strings.ToLower(*title))

	prepositions := []string{"TO", "OR", "OF", "FOR", "AT", "AND", "IN", "BY", "THE"}
	var re *regexp.Regexp
	for _, preposition := range prepositions {
		matchStr := fmt.Sprintf(`\b%s\b`, strings.Title(strings.ToLower(preposition)))
		re = regexp.MustCompile(matchStr)
		*title = re.ReplaceAllString(*title, strings.ToLower(preposition))
	}
	regex := regexp.MustCompile("^THE|^the")
	*title = regex.ReplaceAllString(*title, "The")
	t := *title
	*title = fmt.Sprintf("%s%s", strings.ToUpper(t[0:1]), t[1:len(t)])
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
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
	specialtyID                           string
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
