package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestProperCapitalization(t *testing.T) {

	s := "AT THE REPUBLIC AND STUFF FOR United STATES OF TO TIME"

	CapitalizeTitle(&s)

	if s != "At the Republic and Stuff for United States of to Time" {
		t.Error("invalid capitalization path: \n", s)
	}
}

func TestAbbreviation(t *testing.T) {

	s := "Central Florida Pulmonary Group Pa"
	CapitalizeMedicalAbbreviations(&s)
	if s != "Central Florida Pulmonary Group PA" {
		t.Error("Invalid Substitution\n", s)
	}

	s = "'A' Street Clinic Of Chiropractic, Pllc"
	CapitalizeMedicalAbbreviations(&s)
	if s != "'A' Street Clinic Of Chiropractic, PLLC" {
		t.Error("Invalid Substitution\n", s)
	}

}

func TestCreatePhysicianFromCSV(t *testing.T) {

	fmt.Println(physicianCSV)
	filehandle, err := os.Open(physicianCSV)
	checkErr(err)
	defer filehandle.Close()
	reader := csv.NewReader(filehandle)
	_, err = reader.Read()
	checkErr(err)

	record, err := reader.Read()
	if err != nil {
		panic(err)
	}
	phy := convertCSVRecordToPhysician(record)
	if phy.NPI != record[0] {
		t.Error("Invalid Conversion of physician")
	}
	if phy.CommittedToHeartHealth != record[41] {
		t.Error("Invalid Conversion of physician")
	}
}

func BenchmarkBulkCapitalizeTitle(b *testing.B) {
	b.ReportAllocs()
	fmt.Println(physicianCSV)
	filehandle, err := os.Open(physicianCSV)
	checkErr(err)
	defer filehandle.Close()
	reader := csv.NewReader(filehandle)
	_, err = reader.Read()
	checkErr(err)
	buildSpecialtyArray()

	for i := 0; i < b.N; i++ {
		//err := Unmarshal(reader, &physician)
		//checkErr(err)
		record, err := reader.Read()
		if err != nil {
			panic(err)
		}
		physician = convertCSVRecordToPhysician(record)
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
		//fmt.Println(physician.FirstName, physician.SpecialtyID)
	}
}
