CREATE procedure [ext].[AppointmentData] (
 @sDate DATETIME 
)

AS 

truncate table [AnalyticsMonitoring].[ext].[Appointment_data]

BEGIN

IF OBJECT_ID('tempdb..#apps ') IS NOT NULL DROP TABLE #apps 

--DECLARE @sdate datetime
DECLARE @edate datetime
--SET @sdate='2013-1-31'  
SET @edate=(select min(dateVal) from (select dateadd(month,48,@sdate) as dateVal UNION select dateadd(month,-1,getdate()) as dateVal) a)

set statistics time on 

--select base appoitnment
IF OBJECT_ID('tempdb..#apps ') IS NOT NULL DROP TABLE #apps ;

SELECT distinct 
[appt_patient_id] as pat_id
,appt_id
,appt_provider_id
,appt_location_id
 ,case when appt_kept_ind in ('Y','1') then 1 else 0 end as appt_kept
 ,case when appt_rescheduled_ind='Y' then 1 else 0 end as appt_can
 ,case when [appt_cancel_ind]='Y' then 1 else 0 end as appt_resch
 ,case when appt_kept_ind not in ('Y','1') and appt_rescheduled_ind<>'Y' and  [appt_cancel_ind]<>'Y' then 1 else 0 end as appt_no_show
 ,replace(appt_reason,',','') as appt_reason
 ,cast([appt_date] as date) as appt_date
,case when datepart(hour,coalesce([appt_date],[appt_begintime])) <12 then 'Morning'
	  when datepart(hour,coalesce([appt_date],[appt_begintime])) between 12 and 13 then 'Noon'
	  when datepart(hour,coalesce([appt_date],[appt_begintime]))  between 14 and 18 then 'Late noon' 
	  when datepart(hour,coalesce([appt_date],[appt_begintime])) >18 then 'Evening' END AS Time_of_day
,case when DATEPART ( dw, cast([appt_date] as date) )=1 then 1 else 0 end as weekday_1
,case when DATEPART ( dw, cast([appt_date] as date) )=2 then 1 else 0 end as weekday_2
,case when DATEPART ( dw, cast([appt_date] as date) )=3 then 1 else 0 end as weekday_3
,case when DATEPART ( dw, cast([appt_date] as date) )=4 then 1 else 0 end as weekday_4
,case when DATEPART ( dw, cast([appt_date] as date) )=5 then 1 else 0 end as weekday_5
,case when DATEPART ( dw, cast([appt_date] as date) )=6 then 1 else 0 end as weekday_6
,case when DATEPART ( dw, cast([appt_date] as date) )=7 then 1 else 0 end as weekday_7
,case when datediff(day, appt_create_timestamp, appt_date) = 0 then 0
	  when datediff(day, appt_create_timestamp, appt_date) =1 then 1 
	  when datediff(day, appt_create_timestamp, appt_date) between 2 and 14 then 2
	  when datediff(day, appt_create_timestamp, appt_date) > 14 then 3 end as days_made_in_advance
,case when datepart(month,appt_date) in (1,2,3) then 'Winter'
	  when datepart(month,appt_date) between 4 and 6 then 'Spring'
	  when datepart(month,appt_date) between 7 and 9 then 'Summer'
	  when datepart(month,appt_date) between 10 and 12 then ' fall' END AS Season

,ROW_NUMBER() over (partition by appt_patient_id order by appt_date desc) rownum
into #apps 
 FROM QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[t_appointment] a 
 WHERE [appt_delete_ind]='N'
 AND [appt_date] > @sdate 
 AND [appt_date] < @edate


--select base appoitnment
IF OBJECT_ID('tempdb..#app_prev') IS NOT NULL DROP TABLE #app_prev ;
select distinct
a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_location_id
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.appt_reason
,a.appt_date
,a.Time_of_day
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.Season
,max(case when a.appt_provider_id=b.appt_provider_id then 1 else 0 end) as first_appt_w_prov
,max(case when b.appt_kept=1 and b.rownum-1=a.rownum then 1 else 0 end) as last_appt_kept
,max(case when b.appt_can=1 and b.rownum-1=a.rownum then 1 else 0 end) as last_appt_can
,max(case when b.appt_resch=1 and b.rownum-1=a.rownum then 1 else 0 end) as last_appt_resc
,max(case when b.appt_no_show=1 and b.rownum-1=a.rownum then 1 else 0 end) as last_appt_no_show
 --,min(datediff(day,b.appt_date,a.appt_date)) as days_last_appointment
 ,min(case when datediff(day,b.appt_date,a.appt_date) = 0 then 0
		   when datediff(day,b.appt_date,a.appt_date) = 1 then 1
		   when datediff(day,b.appt_date,a.appt_date) between 2 and 14 then 2
		   when datediff(day,b.appt_date,a.appt_date) >14  then 3 end ) as days_last_appointment
 ,count(distinct b.appt_id) as previous_appointments
 ,sum(case when b.[appt_date] > DATEADD(year,-1,a.[appt_date]) then 1 else 0 end ) as appointment_within_year
 , avg(cast(b.appt_kept as numeric)) as kept_perc
 ,avg(cast(b.appt_can as numeric)) as can_perc
 ,avg(cast(b.appt_resch as numeric)) as resch_perc
 ,avg(cast(b.appt_no_show as numeric)) as no_show_perc
 ,max(case when b.appt_id is null then 1 else 0 end) as no_prev_appt
into #app_prev
  from #apps  a
  left join #apps  b
  on a.pat_id=b.pat_id
  and a.[appt_date]>b.[appt_date]
  --left join QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[t_patient] p
  --on b.pat_id=p.pat_id
  --and p.pat_delete_ind='N'
group by 
a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_location_id
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.appt_reason
,a.appt_date
,a.Time_of_day
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.Season
,a.rownum


--Additional appoitnment data
IF OBJECT_ID('tempdb..#app_stats') IS NOT NULL DROP TABLE #app_stats;
SELECT distinct
a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_location_id
,a.previous_appointments
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.appt_reason
,a.appt_date
,a.Time_of_day
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.season
,a.days_made_in_advance
,a.days_last_appointment
,a.first_appt_w_prov
,case when a.appointment_within_year = 0 then 0 
	  when a.appointment_within_year = 1 then 1 
	  when a.appointment_within_year > 1 then 2 end as appt_within_year
,last_appt_kept
,last_appt_can
,last_appt_resc
,last_appt_no_show
,a.kept_perc
,a.can_perc
,a.resch_perc
,a.no_show_perc
,a.no_prev_appt
,c.lat as appt_lat
,c.long as appt_lng
INTO #app_stats
FROM #app_prev a
LEFT JOIN QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[location_master] l
ON a.appt_location_id=l.location_id
LEFT JOIN  [AnalyticsMonitoring].[ext].[location_cords] c
ON c.name=l.location_display_name
AND c.source='bidco'





--Get base patients 
IF OBJECT_ID('tempdb..#pats') IS NOT NULL DROP TABLE #pats;
 SELECT distinct
 p.pat_id
 ,pat_responsible_provider_id
 ,pat_date_of_birth
 ,pat_income
 ,case when pat_family_size=1 then 1
	   when pat_family_size > 1 then 2 end as pat_family_size
 ,pat_zip
 ,case when [pat_ethnicity] ='Hispanic or Latino' or [pat_ethnicity]='Hispanic/Latino' or [pat_race]='Hispanic or Latino' then 1 ELSE 0 End as Ethnicity
 ,case when [pat_race] ='Black or African American' or [pat_race]='Black/African American' then 1
       when [pat_race] ='White'  then 2
       when [pat_race] ='Asian' then 3 else 4 end as Race
 ,case when [pat_language]='English' then 1 
	   when [pat_language]='Chinese' then 2
       when [pat_language]='Spanish' or  [pat_language]='Spanish; Castilian' then 3 ELSE 4 END as [language]
 ,case when[pat_sex] ='M' then 1 else 0 end as Gender
 into #pats
 FROM QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[t_patient] p
 JOIN #apps a
 ON a.pat_id=p.pat_id
  WHERE p.pat_delete_ind='N'

--Get income and coordinates for each patient
IF OBJECT_ID('tempdb..#pat_stats1') IS NOT NULL DROP TABLE #pat_stats1;
SELECT distinct
p.pat_id
,p.pat_responsible_provider_id
,p.pat_date_of_birth
,c.[LAT] as pat_lat
,c.[LNG] as pat_lng
,case when p.pat_income IS NULL or p.pat_income='0' or p.pat_income='0.00' then cast(round(z.income,0) as bigint) else cast(round(p.pat_income,0) as bigint) end as income
,pat_family_size
,p.ethnicity
,p.race
,p.[language]
,p.Gender
INTO #pat_stats1
  FROM #pats p
  LEFT JOIN [AnalyticsMonitoring].[ext].[wrk_zip_cost] z  
  ON z.zip=left(p.pat_zip,5)
  LEFT JOIN [AnalyticsMonitoring].[ext].[zip_cords] c
  on c.zip=left(p.pat_zip,5)



  --Breakout patient status
IF OBJECT_ID('tempdb..#pat_stats2') IS NOT NULL DROP TABLE #pat_stats2;
  SELECT distinct
 p.pat_id
,p.pat_responsible_provider_id
,p.pat_date_of_birth
,p.pat_lat
,p.pat_lng
,case when p.income
 between 1 and 20000 then 1 
	  when p.income between 20001 and 60000 then 2
	  when p.income > 60000 then 3 end as income
,pat_family_size
,p.ethnicity
,p.race
,p.[language]
,p.gender
INTO #pat_stats2
 FROM #pat_stats1 p






   --Get event codes for relevant diagnosises 
IF OBJECT_ID('tempdb..#events') IS NOT NULL DROP TABLE #events;
 SELECT distinct [EventCodeID]
,case when CodeValue like '296%' or CodeValue like '311%' then 1 else 0 end as depression
,case when CodeValue like 'v22%' or CodeValue like '650%' or CodeValue='59400' or CodeValue='59510'then 1 else 0 end as preg
,case when CodeValue like '300%' then 1 else 0 end as anxiety
,case when CodeValue ='305.1' then 1 else 0 end as tobacco
, case when CodeValue ='434.91' then 1 else 0 end as stroke
,case when CodeValue like '304%' then 1 else 0 end as drug_dep
, case when CodeValue  like '308%' then 1 else 0 end as pain
,case when CodeValue  like '250%' then 1 else 0 end as diabetes
INTO #events
FROM QDWSQLPROD03.bidco_WAREHOUSE_PRD.[rpt].[EventCode]
WHERE (CodeValue like '296%'
or CodeValue like 'v22%'
or CodeValue like '311%'
or CodeValue like '650%'
or CodeValue like '300%'
or CodeValue ='305.1'
or CodeValue ='434.91'
or CodeValue like '304%'
or CodeValue like '308%'
or CodeValue like '250%'
) and CodeSet='ICD9'
or 
((CodeValue='59400' or CodeValue='59510')
and CodeSet='CPT')


--Get relevant diagnosis for patients
IF OBJECT_ID('tempdb..#pat_diag') IS NOT NULL DROP TABLE #pat_diag;

SELECT
p.pat_id
,max(depression) as depression
,max(anxiety) as anxiety
,max(stroke) as stroke
,max(pain) as pain
,max(diabetes) as diabetes
,max(case when preg=1 then e.StartDate else NULL end) as preg_date
into #pat_diag
FROM #apps p
JOIN QDWSQLPROD03.bidco_WAREHOUSE_PRD.mpi.person_patient pp
on pp.pat_id=p.pat_id
and pp.active_ind='Y'
JOIN QDWSQLPROD03.bidco_WAREHOUSE_PRD.[rpt].[Event] e
on e.[PersonID]=pp.[person_id]
AND DATEADD(year,-2,@sdate)>e.StartDate
JOIN #events ev
on ev.[EventCodeID]=e.[EventCodeID]
GROUP BY
p.pat_id


--Combine diagnosis and patient information
IF OBJECT_ID('tempdb..#pat_stats3') IS NOT NULL DROP TABLE #pat_stats3;

SELECT distinct
 p.pat_id
,p.pat_responsible_provider_id
,p.pat_date_of_birth
,p.pat_lat
,p.pat_lng
,income
,pat_family_size
,p.ethnicity
,p.race
,p.[language]
,p.gender
,coalesce(d.depression,0)  as depression
,coalesce(d.anxiety,0) as anxiety
,coalesce(d.stroke,0)as stroke
,coalesce(d.pain,0) as pain
,coalesce(d.diabetes,0) as diabetes
,d.preg_date
into #pat_stats3
FROM #pat_stats2 p
LEFT JOIN #pat_diag d
on p.pat_id=d.pat_id



--Combine patients and appoitnments and calculate distance between patient address and appointmnet location
IF OBJECT_ID('tempdb..#pat_appt') IS NOT NULL DROP TABLE #pat_appt;

SELECT distinct
 a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_location_id
,a.appt_reason
,a.appt_date
,a.previous_appointments
,[AnalyticsMonitoring].[ext].[fnCalcDistanceMiles] (a.appt_lat,a.appt_lng,p.pat_lat,p.pat_lng) as distance
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.time_of_day
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.season
,a.days_made_in_advance
,a.days_last_appointment
,a.appt_within_year
,a.last_appt_kept
,a.last_appt_can
,a.last_appt_resc
,a.last_appt_no_show
,coalesce(a.kept_perc,0) as kept_perc
,coalesce(a.can_perc,0) as can_perc
,coalesce(a.resch_perc,0) as resch_perc
,coalesce(a.no_show_perc,0) as no_show_perc
,a.first_appt_w_prov
,case when appt_provider_id=p.pat_responsible_provider_id then 1 else 0 end as appt_w_resp_prov 
,datediff(year,p.pat_date_of_birth,a.appt_date) as age
,p.income
,p.pat_family_size
,p.ethnicity
,p.race
,p.[language]
,p.gender
,p.depression
,p.anxiety
,p.stroke
,p.pain
,p.diabetes
,case when p.preg_date>dateadd(year,-2,a.appt_date) then 1 else 0 end as preg
into #pat_appt
FROM #app_stats a
JOIN #pat_stats3 p
on a.pat_id=p.pat_id


--Get additional infomration on patient appoitnments and breakdown features into categories
IF OBJECT_ID('tempdb..#pat_appt_stats1') IS NOT NULL DROP TABLE #pat_appt_stats1;
SELECT
 a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_location_id
,a.appt_reason
,a.appt_date
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,case when a.distance <=6 then 1 
      when a.distance between 7 and 90 then 2 
      when a.distance > 90 then 3 else 0 end as appt_dist
,a.time_of_day
,a.season
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.days_last_appointment
,a.appt_within_year
,a.last_appt_kept
,a.last_appt_can
,a.last_appt_resc
,a.last_appt_no_show
,case when a.kept_perc=0 and a.previous_appointments>1 then 1 
 when a.kept_perc  between .001 and .2 then 2 
when a.kept_perc between .2001 and .6 then 3 else 4 end as kept_perc

,case when a.can_perc=0 and a.appt_within_year<>1 and a.appt_within_year<>0 then 1 
	  when a.can_perc between .001 and .2 then 2 
	  when a.can_perc between .2001 and .6 then 3 else 4 end as can_perc


,case when a.resch_perc=0 and a.appt_within_year<>1 and a.appt_within_year<>0 then 1 
	  when a.resch_perc between .001 and .2 then 2 
	  when a.resch_perc between .2001 and .6 then 3 else 4 end as resch_perc


,case when a.no_show_perc=0 and a.appt_within_year<>1 and a.appt_within_year<>0 then 1 
	  when a.no_show_perc between .001 and .2 then 2
	  when a.no_show_perc between .2001 and .6 then 3 else 4 end as no_show_perc

,a.first_appt_w_prov
,a.appt_w_resp_prov
,case when a.age < 18 then 1 
	  when a.age between 18 and 24 then 2 
	  when a.age between 25 and 44 then 3 
	  when a.age between 45 and 64 then 4 else 5 end as pat_age
,a.income
,a.pat_family_size
,a.Ethnicity
,a.race
,a.[language]
,a.gender
,a.depression
,a.anxiety
,a.stroke
,a.pain
,a.diabetes
,a.preg
INTO #pat_appt_stats1
FROM
#pat_appt a


--Classify appointment reasons
IF OBJECT_ID('tempdb..#reasons') IS NOT NULL DROP TABLE #reasons;
SELECT distinct
 appt_reason
,case when appt_reason like 'D %' or appt_reason like 'D-%' or appt_reason like 'Den%' or appt_reason like '%Dental%' then 1 else 0 end as appt_reason_dental
,case when appt_reason like 'BH-' or appt_reason like 'BH ' then 1 else 0 end as appt_reason_bh
,case when appt_reason like '%Office Visit%' or appt_reason like '%Off Visit%' or appt_reason like '% OV %' then 1 else 0 end as appt_reason_off_visit
,case when appt_reason like '%Established Patient%' then 1 else 0 end as appt_reason_est_pat
,case when appt_reason like '%new patient%' then 1 else 0 end as appt_reason_new_pat
,case when appt_reason like '%Established Clinic%' then 1 else 0 end as appt_reason_est_clin
,case when appt_reason like '%Well Child%' or  appt_reason like '%WC%' then 1 else 0 end as appt_reason_wc
,case when appt_reason like '%rout%' then 1 else 0 end as appt_reason_rout
INTO #reasons
FROM #pat_appt_stats1

--Classify provider specialties
IF OBJECT_ID('tempdb..#prov_spec') IS NOT NULL DROP TABLE #prov_spec;
SELECT distinct
 p.prov_id
,case when p.prov_specialty_1 ='Family Practice' or p.prov_specialty_unscrubbed like '%Family Practice%' then 1 else 0 end as prov_fam_prac
,case when p.prov_specialty_1 like '%dent%' or p.prov_specialty_unscrubbed like '%dent%' then 1 else 0 end as prov_dent
,case when p.prov_specialty_1 like '%Phys%Assis%' or p.prov_specialty_unscrubbed like '%Phys%Assis%' then 1 else 0 end as prov_phys_asst
,case when p.prov_specialty_1 like '%PCP%' or p.prov_specialty_unscrubbed like '%PCP%' then 1 else 0 end as prov_pcp
,case when p.prov_specialty_1 like '%mental%' or p.prov_specialty_unscrubbed like '%mental%' then 1 else 0 end as prov_mental
,case when p.prov_specialty_1 like '%Gynecology%' or p.prov_specialty_unscrubbed like '%Gynecology%'
 or p.prov_specialty_1 like '%OBGYN%' or p.prov_specialty_unscrubbed like '%OBGYN%'then 1 else 0 end as prov_obgy
,case when p.prov_specialty_1 like '%Internal Medicine%' or p.prov_specialty_unscrubbed like '%Internal Medicine%' then 1 else 0 end as prov_intrnl_med
,case when p.prov_specialty_1 like '%General Practice%' or p.prov_specialty_unscrubbed like '%General Practice%' then 1 else 0 end as prov_gen_prac
,prov_usual_location_id
 into #prov_spec
 FROM #pat_appt a
 join QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[provider_master] p
on a.appt_provider_id=p.prov_id


--obtain payer_types
  IF OBJECT_ID('tempdb..#payer') IS NOT NULL DROP TABLE #payer;

SELECT
appt_id
,case when payer_type_name = 'Commercial' then 1 
      when payer_type_name = 'Medicaid' then 2 
      when payer_type_name = 'Medicare' then 3 end as Insurance_type
into #payer
from (
SELECT distinct
appt_id,
pat_id
,payer_type_name
,ROW_NUMBER() over (partition by appt_id order by policy_start_dt desc) rownum
 from  #pat_appt_stats1 a
 join  QDWSQLPROD03.bidco_WAREHOUSE_PRD.dbo.t_payer p
 on p.patient_id=a.pat_id
 and policy_start_dt<=appt_date
 and (policy_end_dt>appt_date
 or policy_end_dt is null)
  join QDWSQLPROD03.bidco_WAREHOUSE_PRD.dbo.[Payer_name] pn
  on pn.payer_name_id=p.payer_name_id
  join QDWSQLPROD03.bidco_WAREHOUSE_PRD.dbo.[Payer_type] pt
  on pt.payer_type_id=pn.payer_type_id
  where payer_delete_ind='n'
and payer_type_name in ('Commercial','Medicaid','Medicare')
) a
where rownum=1


--Get appointment reason and payer type at appointment type
IF OBJECT_ID('tempdb..#pat_appt_stats2') IS NOT NULL DROP TABLE #pat_appt_stats2;
SELECT distinct
 a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_reason
,a.appt_date
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,appt_dist
,a.Time_of_day
,a.Season
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.days_last_appointment
,a.appt_within_year
,a.last_appt_kept
,a.last_appt_can
,a.last_appt_resc
,a.last_appt_no_show
,a.kept_perc
,a.resch_perc
,a.can_perc
,a.no_show_perc
,a.first_appt_w_prov
,a.appt_w_resp_prov
,a.pat_age
,a.income
,a.pat_family_size
,a.Ethnicity
,a.race
,a.[language]
,a.Gender
,a.depression
,a.anxiety
,a.stroke
,a.pain
,a.diabetes
,a.preg
,b.appt_reason_dental
,b.appt_reason_bh
,b.appt_reason_off_visit
,b.appt_reason_est_pat
,b.appt_reason_est_clin
,b.appt_reason_new_pat
,b.appt_reason_wc
,b.appt_reason_rout
,case when p.prov_usual_location_id=a.appt_location_id then 1 else 0 end as appt_prov_usual_loc
,p.prov_fam_prac
,p.prov_dent
,p.prov_phys_asst
,p.prov_pcp
,p.prov_mental
,p.prov_obgy
,p.prov_intrnl_med
,p.prov_gen_prac
,py.insurance_type
into #pat_appt_stats2
FROM #pat_appt_stats1 a
JOIN #reasons b
on a.appt_reason=b.appt_reason
LEFT JOIN #prov_spec p
on a.appt_provider_id=p.prov_id
LEFT JOIN #payer py
on py.appt_id=a.appt_id



--Get output
IF OBJECT_ID('tempdb..#output') IS NOT NULL DROP TABLE #output
SELECT
 a.pat_id
,a.appt_id
,a.appt_provider_id
,a.appt_reason
,a.appt_date
,a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.appt_dist
,a.time_of_day
,a.Season
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.days_last_appointment
,a.appt_within_year
,a.last_appt_kept
,a.last_appt_can
,a.last_appt_resc
,a.last_appt_no_show
,a.kept_perc
,a.resch_perc
,a.can_perc
,a.no_show_perc
,a.first_appt_w_prov
,a.appt_w_resp_prov
,a.pat_age
,a.income
,a.pat_family_size
,a.Ethnicity
,a.race
,a.[language]
,a.Gender
,a.depression
,a.anxiety
,a.stroke
,a.pain
,a.diabetes
,a.preg
,a.appt_reason_dental
,a.appt_reason_bh
,a.appt_reason_off_visit
,a.appt_reason_est_pat
,a.appt_reason_est_clin
,a.appt_reason_new_pat
,a.appt_reason_wc
,a.appt_reason_rout
,a.appt_prov_usual_loc
,a.prov_fam_prac
,a.prov_dent
,a.prov_phys_asst
,a.prov_pcp
,a.prov_mental
,a.prov_obgy
,a.prov_intrnl_med
,a.prov_gen_prac
,coalesce(a.insurance_type,0) as Insurance
into #output
FROM #pat_appt_stats2 a



--Get output
INSERT INTO [AnalyticsMonitoring].[ext].[Appointment_data](
	[appt_kept] ,
	[appt_can] ,
	[appt_resch] ,
	[appt_no_show],
	[appt_dist],
	[Time_of_day],
	[Season],
	[weekday_1] ,
	[weekday_2] ,
	[weekday_3] ,
	[weekday_4] ,
	[weekday_5] ,
	[weekday_6] ,
	[weekday_7] ,
	[days_made_in_advance] ,
	[days_last_appointment] ,
	[appt_within_year] ,
	[last_appt_kept] ,
	[last_appt_can] ,
	[last_appt_resc] ,
	[last_appt_no_show] ,
	[kept_perc] ,
	[resch_perc] ,
	[can_perc] ,
	[no_show_perc] ,
	[first_appt_w_prov] ,
	[appt_w_resp_prov] ,
	[pat_age] ,
	[income] ,
	[pat_family_size] ,
	[ethnicity] ,
	[race] ,
	[language] ,
	[gender] ,
	[depression] ,
	[anxiety] ,
	[stroke] ,
	[pain] ,
	[diabetes] ,
	[preg] ,
	[appt_reason_dental] ,
	[appt_reason_bh] ,
	[appt_reason_off_visit] ,
	[appt_reason_est_pat] ,
	[appt_reason_est_clin] ,
	[appt_reason_new_pat] ,
	[appt_reason_wc] ,
	[appt_reason_rout] ,
	[appt_prov_usual_loc] ,
	[prov_fam_prac] ,
	[prov_dent] ,
	[prov_phys_asst] ,
	[prov_pcp] ,
	[prov_mental] ,
	[prov_obgy] ,
	[prov_intrnl_med] ,
	[prov_gen_prac] ,
	[insurance] 
)

SELECT
-- a.pat_id
--,a.appt_id
--,a.appt_provider_id
--,a.appt_reason
--,a.appt_date
a.appt_kept
,a.appt_can
,a.appt_resch
,a.appt_no_show
,a.appt_dist 
,a.Time_of_day
,a.Season
,a.weekday_1
,a.weekday_2
,a.weekday_3
,a.weekday_4
,a.weekday_5
,a.weekday_6
,a.weekday_7
,a.days_made_in_advance
,a.days_last_appointment
,a.appt_within_year
,a.last_appt_kept
,a.last_appt_can
,a.last_appt_resc
,a.last_appt_no_show
,a.kept_perc
,a.resch_perc
,a.can_perc
,a.no_show_perc
,a.first_appt_w_prov
,a.appt_w_resp_prov
,a.pat_age
,a.income
,a.pat_family_size
,a.ethnicity
,a.race
,a.[language]
,a.gender
,case when a.depression+b.depression>0 then 1 else 0 end as depression
,case when a.anxiety+b.anxiety>0 then 1 else 0 end as anxiety
,case when a.stroke+b.stroke>0 then 1 else 0 end as stroke
,case when a.pain+b.pain>0 then 1 else 0 end as pain
,case when a.diabetes+b.diabetes>0 then 1 else 0 end as diabetes
,case when a.preg+b.preg>0 then 1 else 0 end as preg
,a.appt_reason_dental
,a.appt_reason_bh
,a.appt_reason_off_visit
,a.appt_reason_est_pat
,a.appt_reason_est_clin
,a.appt_reason_new_pat
,a.appt_reason_wc
,a.appt_reason_rout
,a.appt_prov_usual_loc
,a.prov_fam_prac
,a.prov_dent
,a.prov_phys_asst
,a.prov_pcp
,a.prov_mental
,a.prov_obgy
,a.prov_intrnl_med
,a.prov_gen_prac
,coalesce(a.insurance,0) as insurance
--into ext.model_train
from #output a
  left join  (
	  select patient_id,max(depression) as depression, max(preg) as preg, max(anxiety) as anxiety, max(tobacco) as tobacco, max(stroke) as stroke
	  ,max(drug_dep) as drug_dep, max(pain) as pain, max(diabetes) as diabetes
	   from (
			select distinct patient_id, case when icd9_code like '296%' or icd9_code like '311%' then 1 else 0 end as depression, case when icd9_code like 'v22%' or icd9_code like '650%' then 1 else 0 end as preg
			, case when icd9_code like '300%' then 1 else 0 end as anxiety, case when icd9_code ='305.1' then 1 else 0 end as tobacco, case when icd9_code ='434.91' then 1 else 0 end as stroke
			, case when icd9_code like '304%' then 1 else 0 end as drug_dep, case when icd9_code  like '308%' then 1 else 0 end as pain
			 , case when icd9_code  like '250%' then 1 else 0 end as diabetes
			from
			QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].t_problem  p
			where icd9_code like '296%'
			or icd9_code like 'v22%'
			or icd9_code like '311%'
			or icd9_code like '650%'
			or icd9_code like '300%'
			or icd9_code ='305.1'
			or icd9_code ='434.91'
			or icd9_code like '304%'
			or icd9_code like '308%'
			or icd9_code like '250%'
			union
			select distinct patient_id, case when icd9_code like '296%' or icd9_code like '311%' then 1 else 0 end as depression, case when icd9_code like 'v22%' or icd9_code like '650%' then 1 else 0 end as preg
			, case when icd9_code like '300%' then 1 else 0 end as anxiety, case when icd9_code ='305.1' then 1 else 0 end as tobacco, case when icd9_code ='434.91' then 1 else 0 end as stroke
			, case when icd9_code like '304%' then 1 else 0 end as drug_dep, case when icd9_code  like '308%' then 1 else 0 end as pain
			  , case when icd9_code  like '250%' then 1 else 0 end as diabetes
			--, assessment_date as diag_date 
			from
			QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].t_assessment ta
			where icd9_code like '296%'
			or icd9_code like 'v22%'
			or icd9_code like '311%'
			or icd9_code like '650%'
			or icd9_code like '300%'
			or icd9_code ='305.1'
			or icd9_code ='434.91'
			or icd9_code like '304%'
			or icd9_code like '308%'
			or icd9_code like '250%'
	  ) a group by patient_id
  ) b
  on b.patient_id=a.pat_id


--   IF not EXISTS
--    (
--select @mrn, @personID
--where (@mrn <>'' and @mrn is not null)
--or(@personID is not null and @personID <> '' and @personID <> 0)
--    )
--    BEGIN
--set @mrn='none'
--   END

--prov specialty
--payer type
--previous percentages seperate into features to distinguish no previous history

--select  count(*),prov_specialty_unscrubbed
--from  QDWSQLPROD03.bidco_WAREHOUSE_PRD.[dbo].[provider_master] p

--group by prov_specialty_unscrubbed
--order by count(*) desc


--Family Practice
----prov_usual_location_id



--SELECT avg(cast(appt_no_show as decimal (13,3))), appt_reason, count(*) FROM #pat_appt a
--group by appt_reason
--order by count(*) desc





set statistics time off

END