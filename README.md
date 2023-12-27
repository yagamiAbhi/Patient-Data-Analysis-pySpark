# Patient-Data-Analysis-pySpark
Patient Data Analysis using Pyspark

Heathy-You is an international health care company working with various other hospitals globally. The company has a huge collection of the patients registering for consultation and operation at different centers.

Calculate the final fee to be paid by each patient during registration. It requires us to check whether a registration has insurance cover coming under the collaborated companies or not. So technical team has decided to store data on to HDFS and use Spark core and Spark SQL for processing the fee structure for calculating the final fee for a particular registration.

In the process of getting final fee, hospital must verify the below business rules:

Check patient’s insurance id, background status and discharge approval.

List of companies with which hospital has collaborated for insurance and reimbursement.


**The company stores the data in the following datasets.**

**File Name: PatientRegistrationData.csv**

This file contains registration data provided by the patients at the time of visit/registration.

Schema: RegNo: STRING, PatientName: STRING, Age: STRING, DBO: STRING, Gender: STRING, BloodG: STRING, Department: STRING, Doctor: STRING, InsuranceId: STRING, BackgroundStatus: STRING, DischargeApproval: STRING


**File Name: CompanyInsuranceData.csv**

This file contains details of those companies who have tie up with the hospital.

Schema: CompanyName: STRING, InsuranceId: STRING, PercentCover: INT, Limit: LONG


**File Name: BillTillDate.txt**

This file contains total amount consumed during patient’s consultation process.

Schema: RegNo: STRING, AmountPaid: LONG, TotalAmount: LONG

 

**Implement the below data analysis requirements:**

1. Create three RDDs patientRegistrationRDD, billTillDateRDD and companyInsuranceRDD by consuming data from PatientRegistrationData.csv, BillTillDate.txt and CompanyInsuranceData.csv files respectively.

2. Create a separate RDD out of patientRegistrationRDD as patientToBeDischargedRDD for those patients whose background status is “COMPLETED” and discharge approval is “POSITIVE”

3. Convert the four RDDs into Spark SQL Data Frames. patientRegistrationRDD to patientRegistrationDf, billTillDateRDD to billTillDateDf, companyInsuranceRDD to companyInsuranceDf and patientToBeDischargedRDD to patientToBeDischargedDf

4. Check whether InsuranceId in patientToBeDischargedDf matches with any InsuranceId in companyInsuranceDf. Consider the patient with RegNo=18 for this requirement

5. Store the details of patient to ApprovedDf, if the InsuranceId in companyInsuranceDf and background status is “COMPLETED” and discharge approval is “POSITIVE”.

6. For approved patient, calculate the remaining amount to be paid from BillTillDate data.

7. Calculate the amount covered by the insurance as per the percentage cover given by the company up to a certain limit.

8. Calculate the final fee by deducting the insurance covered amount from the remaining amount.

9. Print RegNo, PatientName, Age, DBO, Gender, BloodG, Department, Doctor, CompanyName, FinalBillAmount on the console and save the details in HDFS.

10. Create RDD with the data generated from question 9. Find out the total amount claimed from each company by Healthy-You Patients. (Use GroupByKey() as well as ReduceByKey() transformations and see which has better performance.)
