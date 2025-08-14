#!/usr/bin/env python3
"""
Generate a single flat HR CSV with 500,000 rows matching the requested schema.

Key guarantees:
- One big CSV (hr_dataset_500k.csv), chunk-written for memory efficiency.
- Every categorical text column has a matching *_ID column in the same row.
- Manager is also an employee (ManagerID exists among EmployeeIDs, no self-manager).
- Dates are logically consistent: DOB < HireDate, promotions/reviews after Hire, termination (if any) after Hire, certification & expiry logic valid, etc.
- TenureMonth computed from HireDate to (TerminationDate or today).
- Duplicate fields requested in the spec (e.g., HireDate & DateOfHire; Feedback/Feedback_ID & Feedback/FeedbackID) are included as-is.

You can adjust TOTAL_ROWS, CHUNK_SIZE, TODAY (to freeze time), and lookup lists as needed.
"""

import csv
import random
import math
import datetime as dt
import os
from collections import defaultdict

# ----------------------------
# CONFIG
# ----------------------------
TOTAL_ROWS = 500_000
CHUNK_SIZE = 50_000
OUTPUT_FILE = "hr_dataset_500k.csv"

SEED = 42
random.seed(SEED)

# "Today" anchor for tenure & date sanity
TODAY = dt.date.today()

# Proportion of managers (roughly)
MANAGER_RATIO = 0.12  # ~12% of employees act as people managers

# Optional: set to True for reproducible DOB/Hire windows for all rows
STRICT_REPRO = True

# ----------------------------
# FAKE DATA SUPPORT (optional)
# ----------------------------
try:
    from faker import Faker
    faker = Faker()
    Faker.seed(SEED)
    USE_FAKER = True
except Exception:
    USE_FAKER = False

FIRST_NAMES = [
    "Aarav","Vivaan","Aditya","Vihaan","Arjun","Reyansh","Muhammad","Sai","Krishna","Ishaan",
    "Aanya","Diya","Myra","Aadhya","Anaya","Pari","Sara","Ira","Anika","Navya"
]
LAST_NAMES = [
    "Sharma","Verma","Gupta","Patel","Khan","Singh","Mishra","Nair","Iyer","Reddy",
    "Das","Joshi","Gowda","Kulkarni","Bose","Chatterjee","Banerjee","Shetty","Roy","Pillai"
]

CITIES = ["Pune","Mumbai","Bengaluru","Hyderabad","Chennai","Delhi","Kolkata","Ahmedabad","Jaipur","Noida"]
COUNTRIES = ["India","USA","UK","Germany","Singapore","UAE","Canada","Australia"]

BANKS = ["HDFC Bank","ICICI Bank","SBI","Axis Bank","Kotak Mahindra Bank","Yes Bank","Bank of Baroda","Canara Bank"]
DEPARTMENTS = [
    "Engineering","HR","Finance","Sales","Marketing","Operations","IT Support","Legal","Admin","R&D","Procurement"
]
JOB_TITLES = [
    "Software Engineer","Senior Software Engineer","Lead Engineer","Data Analyst","Senior Data Analyst",
    "HR Executive","HR Manager","Finance Executive","Finance Manager","Sales Executive","Sales Manager",
    "DevOps Engineer","Product Manager","QA Engineer","UX Designer"
]
EMPLOYMENT_STATUSES = ["Active","On Leave","Probation","Terminated","Retired"]
EDU_LEVELS = ["High School","Diploma","Bachelor's","Master's","MBA","PhD"]
MARITAL_STATUSES = ["Single","Married","Divorced","Widowed"]
CERTIFICATIONS = [
    "AWS Solutions Architect","Azure Data Engineer","PMP","Scrum Master","ITIL Foundation",
    "Power BI Data Analyst","Google Data Engineer","Cisco CCNA","Salesforce Admin"
]
EMPLOYMENT_TYPES = ["Full-Time","Part-Time","Contract","Intern"]
CERT_STATUS = ["Valid","Expired","In Progress","Revoked"]
TRAINING_PROGRAMS = [
    "Onboarding Bootcamp","Advanced Python","Leadership 101","Cloud Fundamentals",
    "Sales Mastery","Financial Modeling","Power BI Deep Dive","Kubernetes Basics"
]
SKILL_CATEGORIES = ["Programming","Analytics","Leadership","Communication","Cloud","Security","Design","DevOps","Domain"]
REVIEWER_ROLES = ["Manager","Senior Manager","Director","HR Reviewer","Peer"]
LEARNING_PATHS = ["Data Engineering","Data Analytics","Software Development","Cloud Engineer","Sales Leadership","Finance Pro"]
CERT_PROVIDERS = ["AWS","Microsoft","PMI","Scrum Alliance","Axelos","Google","Cisco","Salesforce"]
CERT_LEVELS = ["Associate","Professional","Expert","Foundation","Advanced"]
REVIEW_PERIODS = ["Q1","Q2","Q3","Q4","Mid-Year","Annual"]
PROMO_ELIGIBILITY = ["Eligible","Not Eligible","On Hold"]
REVIEW_TYPES = ["Self Review","Manager Review","360 Review","Calibration"]
REVIEW_STATUS = ["Pending","Submitted","Approved","Rejected"]
REVIEW_LOCATIONS = CITIES
COMPETENCY_CATEGORIES = ["Technical","Behavioral","Leadership","Functional"]
GOAL_CATEGORIES = ["Productivity","Quality","Learning","Innovation","Customer"]
FEEDBACK_TYPES = ["Kudos","Constructive","Performance","Process","Culture"]
TRAINING_RECOMMENDATIONS = ["Advanced Course","Mentorship","Workshop","Certification","No Action"]
IMPROVEMENT_PLANS = ["None","PIP - 30 Days","PIP - 60 Days","Coaching"]
INTERNAL_PROMO = ["Yes","No"]
INITIATIVE_PARTICIPATION = ["High","Medium","Low","None"]
PERFORMANCE_PREFS = ["Individual Contributor","Managerial","Hybrid"]
MENTAL_HEALTH_USAGE = ["Yes","No"]
CULTURAL_FIT = ["High","Medium","Low"]
TEAM_DYNAMICS = ["Cohesive","Collaborative","Siloed","Conflicted"]
FEEDBACK_SHORT = ["Great team player","Needs improvement in deadlines","Excellent communicator","Strong domain knowledge","Proactive"]
ENGAGEMENT_LEVELS = ["Low","Medium","High","Very High"]
TRANSFER_STATUS = ["No","In Progress","Completed"]
MOBILITY = ["Onsite","Hybrid","Remote"]
WORKPLACE_SUPPORT_LEVEL = ["Low","Medium","High"]
POLICY_VIOLATIONS = ["None","Warning","Suspended"]
COMPLIANCE_TRAINING_STATUS = ["Completed","Pending","Overdue"]
CAREER_PATHS = ["IC Track","Manager Track","Specialist Track"]
RECRUITMENT_CHANNELS = ["LinkedIn","Naukri","Referral","Campus","Agency","Indeed"]
PROJECTS = ["Atlas","Neptune","Apollo","Hermes","Zephyr","Orion","Helios","Titan"]
ASSETS = ["Laptop","Desktop","Thin Client","MacBook","iPad"]
SHIFTS = ["General","Morning","Evening","Night","Rotational"]
LEAVE_TYPES = ["Sick","Casual","Privilege","Maternity","Paternity","Unpaid"]
LEAVE_STATUS = ["Approved","Pending","Rejected","Cancelled"]
ATTENDANCE_STATUS = ["Present","Absent","WFH","Half-Day","OD"]  # OD = On Duty
TRAINING_TYPES = ["Classroom","Online","Blended"]
TRAINING_LOCATIONS = CITIES
EXIT_REASONS = ["Career Growth","Compensation","Relocation","Higher Studies","Personal","Performance"]
SALARY_CURRENCIES = ["INR","USD","EUR","GBP","AED","SGD","AUD","CAD"]

# ----------------------------
# ID helper
# ----------------------------
def id_for(value_list, value):
    # return 1-based index
    return value_list.index(value) + 1

# ----------------------------
# Name helpers
# ----------------------------
def rand_name():
    if USE_FAKER:
        fn = faker.first_name()
        ln = faker.last_name()
    else:
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
    return fn, ln

def rand_city():
    return random.choice(CITIES)

def rand_country():
    return random.choice(COUNTRIES)

# ----------------------------
# Date helpers
# ----------------------------
def random_date_between(start: dt.date, end: dt.date) -> dt.date:
    if start >= end:
        return start
    days = (end - start).days
    return start + dt.timedelta(days=random.randint(0, days))

def months_between(d1: dt.date, d2: dt.date) -> int:
    if d2 < d1:
        return 0
    return (d2.year - d1.year) * 12 + (d2.month - d1.month) - (0 if d2.day >= d1.day else 1)

# ----------------------------
# Manager assignment strategy
# ----------------------------
def build_manager_pool(total):
    # Choose ~MANAGER_RATIO of early employees to act as managers for a pyramid-like structure
    pool_size = max(1, int(total * MANAGER_RATIO))
    # Managers will be from first 40% of the IDs to keep hierarchy clean
    upper_bound = max(pool_size, int(total * 0.4))
    manager_ids = list(range(1, upper_bound + 1))
    random.shuffle(manager_ids)
    return set(manager_ids[:pool_size])

def pick_manager_for(emp_id, manager_pool):
    # pick a manager ID different from emp_id and less than emp_id (to avoid forward refs)
    # fallback to nearest lower manager if needed
    candidates = [m for m in manager_pool if m < emp_id]
    if not candidates:
        # for first few employees, just pick 1 if available and not self
        return 1 if emp_id != 1 else (2 if 2 in manager_pool else None)
    return random.choice(candidates)

# ----------------------------
# CSV header
# ----------------------------
HEADERS = [
    "EmployeeID",
    "FirstName","LastName",
    "Gender","GenderID",
    "DateOfBirth",
    "DateOfHire",
    "Department","DepartmentID",
    "JobTitle","JobTitleID",
    "EmploymentStatus","EmploymentStatusID",
    "Location","LocationID",
    "EducationLevel","EducationLevelID",
    "MaritalStatus","MaritalStatusID",
    "Nationality","NationalityID",
    "CertificationName","CertificationID",
    "Employment Type","EmploymentTypeID",
    "BaseSalary",
    "Bonus",
    "TenureMonth",
    "CertificationDate",
    "CertificationStatus","CertificationStatusID",
    "Training Program","TrainingProgramID",
    "TrainingHours",
    "TrainingScore",
    "SkillCategory","SkillCategoryID",
    "ReviewerRole","ReviewerRoleID",
    "SkillAssessmentScore",
    "LearningPath","LearningPathID",
    "CertificationProvider","CertificationProviderID",
    "CertificationLevel","CertificationLevelID",
    "TrainingFeedbackScore",
    "RenewalRequired",
    "RenewalQueDate",
    "ReviewPeriod","ReviewPeriodID",
    "Promotion Eligibility","PromotionEligibilityID",
    "ReviewType","ReviewTypeID",
    "ReviewStatus","ReviewStatusID",
    "ReviewLocation","ReviewLocationID",
    "PerformanceScore",
    "CompetencyCategory","CompetencyCategoryID",
    "GoalCategory","GoalCategoryID",
    "Feedback Type","FeedbackTypeID",
    "TrainingRecommendation","TrainingRecommendationID",
    "HireDate",  # duplicate per requirements (same as DateOfHire)
    "TerminationDate",
    "LastPromotionDate",
    "GoalAchievementPercent",
    "ReviewApproveDate",
    "ReviewSubmissionDate",
    "ReviewerName","ReviewerID",
    "StressLevelScore",
    "ImprovementPlan","ImprovementPlanID",
    "PromotionCount",
    "TeamSize",
    "WorkFromHomeDays",
    "RetentionScore",
    "EngagementScore",
    "InternalPromotion","InternalPromotionID",
    "Participation_in_Initiatives","InitiativeParticipationID",
    "PerformancePreference","PerformancePreferenceID",
    "MentalHealth_Support_Usage","MentalHealthSupportUsageID",
    "CulturalFit","CulturalFitID",
    "TeamDynamics","TeamDynamicsID",
    "Feedback","FeedbackID",
    "AbsenceDays",
    "EngagementLevel","EngagementLevelID",
    "InternalTransferStatus","InternalTransferStatusID",
    "Mobility","MobilityID",
    "WorkplaceSupport_Level","WorkplaceSupportLevelID",
    "Policy_Violations","PolicyViolationID",
    "ComplianceTrainingStatus","ComplianceTrainingStatusID",
    "CareerPath","CareerPathID",
    "Total Work Experience ","Work experience ID",
    "Recruitment_Channel","Recruitment_Channel_ID",
    "Project","Project_ID",
    "Asset","Asset_ID",
    "Shift","Shift_ID",
    "Leave_Type","Leave_Type_ID",
    "Leave_Status","Leave_Status_ID",
    "Attendance_Status","Attendance_Status_ID",
    "Bank","Bank_ID",
    "Trainer Name","Trainer_ID",
    "Training_Type","Training_Type_ID",
    "Training_Location","Training_Location_ID",
    "Feedback","Feedback_ID",  # second Feedback duplicate as requested
    "Exit_Reason","Exit_Reason_ID",
    "Days_Worked","Days_Worked_ID",
    # ---- Added useful HR fields ----
    "ManagerID","ManagerName",
    "SalaryCurrency",
    "CertificationExpiryDate"
]

# ----------------------------
# Main row generator
# ----------------------------
def generate_rows(start_id, end_id, manager_pool):
    # cache reviewer names & ids (simple mapping)
    reviewer_names = {}
    rows = []

    for emp_id in range(start_id, end_id + 1):
        # Names, Gender
        fn, ln = rand_name()
        gender = random.choice(["Male","Female","Other"])
        gender_id = ["Male","Female","Other"].index(gender) + 1

        # Nationality/Location
        nationality = rand_country()
        nationality_id = id_for(COUNTRIES, nationality)
        location = rand_city()
        location_id = id_for(CITIES, location)

        # Department / Title / Status / Employment type
        dept = random.choice(DEPARTMENTS)
        dept_id = id_for(DEPARTMENTS, dept)
        title = random.choice(JOB_TITLES)
        title_id = id_for(JOB_TITLES, title)
        emp_status = random.choices(EMPLOYMENT_STATUSES, weights=[75,5,8,10,2], k=1)[0]
        emp_status_id = id_for(EMPLOYMENT_STATUSES, emp_status)
        emp_type = random.choices(EMPLOYMENT_TYPES, weights=[85,5,8,2], k=1)[0]
        emp_type_id = id_for(EMPLOYMENT_TYPES, emp_type)

        # DOB (1965..2001) -> age ~24..60
        dob_start = dt.date(1965,1,1)
        dob_end = dt.date(2001,12,31)
        dob = random_date_between(dob_start, dob_end)

        # HireDate after 18th birthday, before TODAY
        earliest_hire = max(dob + dt.timedelta(days=18*365), dt.date(2005,1,1))
        latest_hire = min(TODAY, dt.date(TODAY.year, TODAY.month, TODAY.day))
        if earliest_hire > latest_hire:
            earliest_hire = latest_hire - dt.timedelta(days=30)
        hire_date = random_date_between(earliest_hire, latest_hire)

        # Promotions & Reviews after hire
        last_promo = random_date_between(hire_date, TODAY)
        # Sometimes no promotion (use hire_date)
        if random.random() < 0.35:
            last_promo = hire_date

        # Termination (some fraction)
        termination_date = None
        if emp_status in ["Terminated","Retired"]:
            # ensure after hire
            termination_date = random_date_between(max(hire_date, last_promo), TODAY)

        # Certification data (post-hire)
        has_cert = random.random() < 0.75
        cert_name = random.choice(CERTIFICATIONS) if has_cert else ""
        cert_id = id_for(CERTIFICATIONS, cert_name) if has_cert else 0
        cert_date = random_date_between(hire_date, TODAY) if has_cert else None
        cert_status = random.choice(CERT_STATUS) if has_cert else "In Progress"
        cert_status_id = id_for(CERT_STATUS, cert_status)
        cert_provider = random.choice(CERT_PROVIDERS) if has_cert else ""
        cert_provider_id = id_for(CERT_PROVIDERS, cert_provider) if has_cert else 0
        cert_level = random.choice(CERT_LEVELS) if has_cert else ""
        cert_level_id = id_for(CERT_LEVELS, cert_level) if has_cert else 0

        # Certification expiry: typically 1-3 years after cert_date
        if has_cert and cert_date:
            expiry_years = random.choice([1,2,3])
            try:
                cert_expiry = cert_date.replace(year=cert_date.year + expiry_years)
            except ValueError:
                cert_expiry = cert_date + dt.timedelta(days=365*expiry_years)
        else:
            cert_expiry = None

        # Renewal required if expired or expiring soon
        renewal_required = "Yes" if (cert_expiry and cert_expiry <= TODAY + dt.timedelta(days=60)) else "No"
        renewal_queue_date = None
        if renewal_required == "Yes" and cert_date:
            renewal_queue_date = random_date_between(max(cert_date, TODAY - dt.timedelta(days=60)), TODAY + dt.timedelta(days=60))

        # Training info
        training_prog = random.choice(TRAINING_PROGRAMS)
        training_prog_id = id_for(TRAINING_PROGRAMS, training_prog)
        training_hours = random.randint(4, 80)
        training_score = random.randint(50, 100)
        training_feedback_score = random.randint(1, 5)
        training_type = random.choice(TRAINING_TYPES)
        training_type_id = id_for(TRAINING_TYPES, training_type)
        training_loc = random.choice(TRAINING_LOCATIONS)
        training_loc_id = id_for(TRAINING_LOCATIONS, training_loc)

        # Skills & Review meta
        skill_cat = random.choice(SKILL_CATEGORIES)
        skill_cat_id = id_for(SKILL_CATEGORIES, skill_cat)
        reviewer_role = random.choice(REVIEWER_ROLES)
        reviewer_role_id = id_for(REVIEWER_ROLES, reviewer_role)
        skill_assessment = random.randint(1, 100)
        learning_path = random.choice(LEARNING_PATHS)
        learning_path_id = id_for(LEARNING_PATHS, learning_path)

        review_period = random.choice(REVIEW_PERIODS)
        review_period_id = id_for(REVIEW_PERIODS, review_period)
        review_type = random.choice(REVIEW_TYPES)
        review_type_id = id_for(REVIEW_TYPES, review_type)
        review_status = random.choice(REVIEW_STATUS)
        review_status_id = id_for(REVIEW_STATUS, review_status)
        review_location = random.choice(REVIEW_LOCATIONS)
        review_location_id = id_for(REVIEW_LOCATIONS, review_location)

        # Review dates (post-hire)
        review_submit_date = random_date_between(hire_date, TODAY)
        review_approve_date = random_date_between(review_submit_date, TODAY)

        # Reviewer (cache by reviewer_id)
        reviewer_id = random.randint(1, 500)  # arbitrary reviewer pool
        if reviewer_id not in reviewer_names:
            if USE_FAKER:
                reviewer_names[reviewer_id] = f"{faker.first_name()} {faker.last_name()}"
            else:
                fn2, ln2 = rand_name()
                reviewer_names[reviewer_id] = f"{fn2} {ln2}"
        reviewer_name = reviewer_names[reviewer_id]

        # Performance & goals
        performance_score = random.randint(1, 5)
        competency_cat = random.choice(COMPETENCY_CATEGORIES)
        competency_cat_id = id_for(COMPETENCY_CATEGORIES, competency_cat)
        goal_cat = random.choice(GOAL_CATEGORIES)
        goal_cat_id = id_for(GOAL_CATEGORIES, goal_cat)
        goal_achievement = random.randint(50, 120)  # allow >100 for overachievement
        stress_level = random.randint(1, 10)
        improvement_plan = random.choices(IMPROVEMENT_PLANS, weights=[85,7,5,3], k=1)[0]
        improvement_plan_id = id_for(IMPROVEMENT_PLANS, improvement_plan)
        promotion_count = max(0, (last_promo.year - hire_date.year) // random.choice([2,3,4]))
        team_size = random.randint(1, 20)
        wfh_days = random.randint(0, 5)
        retention_score = random.randint(1, 100)
        engagement_score = random.randint(1, 100)
        internal_promo = random.choice(INTERNAL_PROMO)
        internal_promo_id = id_for(INTERNAL_PROMO, internal_promo)
        initiative_participation = random.choice(INITIATIVE_PARTICIPATION)
        initiative_participation_id = id_for(INITIATIVE_PARTICIPATION, initiative_participation)
        performance_pref = random.choice(PERFORMANCE_PREFS)
        performance_pref_id = id_for(PERFORMANCE_PREFS, performance_pref)
        mental_health_usage = random.choice(MENTAL_HEALTH_USAGE)
        mental_health_usage_id = id_for(MENTAL_HEALTH_USAGE, mental_health_usage)
        cultural_fit = random.choice(CULTURAL_FIT)
        cultural_fit_id = id_for(CULTURAL_FIT, cultural_fit)
        team_dynamics = random.choice(TEAM_DYNAMICS)
        team_dynamics_id = id_for(TEAM_DYNAMICS, team_dynamics)

        # Feedbacks (two slots since spec duplicated)
        feedback_text_a = random.choice(FEEDBACK_SHORT)
        feedback_id_a = random.randint(1, 5000)
        feedback_text_b = random.choice(FEEDBACK_SHORT)
        feedback_id_b = random.randint(1, 5000)

        absence_days = random.randint(0, 30)
        engagement_level = random.choice(ENGAGEMENT_LEVELS)
        engagement_level_id = id_for(ENGAGEMENT_LEVELS, engagement_level)
        transfer_status = random.choice(TRANSFER_STATUS)
        transfer_status_id = id_for(TRANSFER_STATUS, transfer_status)
        mobility = random.choice(MOBILITY)
        mobility_id = id_for(MOBILITY, mobility)
        workplace_support = random.choice(WORKPLACE_SUPPORT_LEVEL)
        workplace_support_id = id_for(WORKPLACE_SUPPORT_LEVEL, workplace_support)
        policy_violation = random.choices(POLICY_VIOLATIONS, weights=[97,2,1], k=1)[0]
        policy_violation_id = id_for(POLICY_VIOLATIONS, policy_violation)
        compliance_status = random.choice(COMPLIANCE_TRAINING_STATUS)
        compliance_status_id = id_for(COMPLIANCE_TRAINING_STATUS, compliance_status)
        career_path = random.choice(CAREER_PATHS)
        career_path_id = id_for(CAREER_PATHS, career_path)

        total_work_exp_years = random.randint(0, max(0, (TODAY.year - dob.year) - 18))
        total_work_exp_id = total_work_exp_years  # simple identity ID for numeric bucket

        recruitment_channel = random.choice(RECRUITMENT_CHANNELS)
        recruitment_channel_id = id_for(RECRUITMENT_CHANNELS, recruitment_channel)
        project = random.choice(PROJECTS)
        project_id = id_for(PROJECTS, project)
        asset = random.choice(ASSETS)
        asset_id = id_for(ASSETS, asset)
        shift = random.choice(SHIFTS)
        shift_id = id_for(SHIFTS, shift)
        leave_type = random.choice(LEAVE_TYPES)
        leave_type_id = id_for(LEAVE_TYPES, leave_type)
        leave_status = random.choice(LEAVE_STATUS)
        leave_status_id = id_for(LEAVE_STATUS, leave_status)
        attendance_status = random.choice(ATTENDANCE_STATUS)
        attendance_status_id = id_for(ATTENDANCE_STATUS, attendance_status)
        bank = random.choice(BANKS)
        bank_id = id_for(BANKS, bank)

        trainer_fn, trainer_ln = rand_name()
        trainer_name = f"{trainer_fn} {trainer_ln}"
        trainer_id = random.randint(1, 2000)

        exit_reason = random.choice(EXIT_REASONS) if termination_date else ""
        exit_reason_id = id_for(EXIT_REASONS, exit_reason) if termination_date else 0

        days_worked = random.randint(180, 260)
        days_worked_id = days_worked  # simple numeric id

        # Reviewer name already set; ok.

        # Salary & bonus
        salary_currency = random.choice(SALARY_CURRENCIES)
        # rough salary by title
        base = {
            "Software Engineer": (600000, 1500000),
            "Senior Software Engineer": (1200000, 2500000),
            "Lead Engineer": (1800000, 3500000),
            "Data Analyst": (500000, 1200000),
            "Senior Data Analyst": (900000, 1800000),
            "DevOps Engineer": (1200000, 2400000),
            "Product Manager": (1800000, 3500000),
            "QA Engineer": (600000, 1400000),
            "UX Designer": (800000, 1800000),
            "HR Executive": (400000, 900000),
            "HR Manager": (1000000, 2200000),
            "Finance Executive": (450000, 1000000),
            "Finance Manager": (1200000, 2600000),
            "Sales Executive": (350000, 900000),
            "Sales Manager": (900000, 2200000),
        }
        low, high = base.get(title, (500000, 2000000))
        base_salary = random.randint(low, high)
        bonus = int(base_salary * random.uniform(0.05, 0.25))

        # Manager assignment
        manager_id = pick_manager_for(emp_id, manager_pool)
        if manager_id is None or manager_id == emp_id:
            # fallback to 1 if safe
            manager_id = 1 if emp_id != 1 else 2
        # manager name = a deterministic placeholder (can be resolved after, but for simplicity use synthetic)
        # In real data, you'd map manager_id -> actual name; here we create a readable label:
        manager_name = f"Emp{manager_id:06d}"

        # Tenure (to termination or today)
        tenure_end = termination_date if termination_date else TODAY
        tenure_month = months_between(hire_date, tenure_end)

        # Secondary date alias (spec asks both DateOfHire and HireDate)
        date_of_hire = hire_date

        # Training recommendation & feedback type (remaining)
        training_reco = random.choice(TRAINING_RECOMMENDATIONS)
        training_reco_id = id_for(TRAINING_RECOMMENDATIONS, training_reco)
        feedback_type = random.choice(FEEDBACK_TYPES)
        feedback_type_id = id_for(FEEDBACK_TYPES, feedback_type)

        # ReviewerRole already added; ReviewerName done.

        # Compose row
        rows.append([
            emp_id,
            fn, ln,
            gender, gender_id,
            dob.isoformat(),
            date_of_hire.isoformat(),
            dept, dept_id,
            title, title_id,
            emp_status, emp_status_id,
            location, location_id,
            random.choice(EDU_LEVELS), id_for(EDU_LEVELS, random.choice(EDU_LEVELS)),
            random.choice(MARITAL_STATUSES), id_for(MARITAL_STATUSES, random.choice(MARITAL_STATUSES)),
            nationality, nationality_id,
            cert_name, cert_id,
            emp_type, emp_type_id,
            base_salary,
            bonus,
            tenure_month,
            cert_date.isoformat() if cert_date else "",
            cert_status, cert_status_id,
            training_prog, training_prog_id,
            training_hours,
            training_score,
            skill_cat, skill_cat_id,
            reviewer_role, reviewer_role_id,
            skill_assessment,
            learning_path, learning_path_id,
            cert_provider, cert_provider_id,
            cert_level, cert_level_id,
            training_feedback_score,
            renewal_required,
            renewal_queue_date.isoformat() if renewal_queue_date else "",
            review_period, review_period_id,
            random.choice(PROMO_ELIGIBILITY), id_for(PROMO_ELIGIBILITY, random.choice(PROMO_ELIGIBILITY)),
            review_type, review_type_id,
            review_status, review_status_id,
            review_location, review_location_id,
            performance_score,
            competency_cat, competency_cat_id,
            goal_cat, goal_cat_id,
            feedback_type, feedback_type_id,
            training_reco, training_reco_id,
            hire_date.isoformat(),
            termination_date.isoformat() if termination_date else "",
            last_promo.isoformat(),
            goal_achievement,
            review_approve_date.isoformat(),
            review_submit_date.isoformat(),
            reviewer_name, reviewer_id,
            stress_level,
            improvement_plan, improvement_plan_id,
            promotion_count,
            team_size,
            wfh_days,
            retention_score,
            engagement_score,
            internal_promo, internal_promo_id,
            initiative_participation, initiative_participation_id,
            performance_pref, performance_pref_id,
            mental_health_usage, mental_health_usage_id,
            cultural_fit, cultural_fit_id,
            team_dynamics, team_dynamics_id,
            feedback_text_a, feedback_id_a,
            absence_days,
            engagement_level, engagement_level_id,
            transfer_status, transfer_status_id,
            mobility, mobility_id,
            workplace_support, workplace_support_id,
            policy_violation, policy_violation_id,
            compliance_status, compliance_status_id,
            career_path, career_path_id,
            total_work_exp_years, total_work_exp_id,
            recruitment_channel, recruitment_channel_id,
            project, project_id,
            asset, asset_id,
            shift, shift_id,
            leave_type, leave_type_id,
            leave_status, leave_status_id,
            attendance_status, attendance_status_id,
            bank, bank_id,
            trainer_name, trainer_id,
            training_type, training_type_id,
            training_loc, training_loc_id,
            feedback_text_b, feedback_id_b,
            exit_reason, exit_reason_id,
            days_worked, days_worked_id,
            # Added columns
            manager_id, manager_name,
            salary_currency,
            cert_expiry.isoformat() if cert_expiry else ""
        ])

    return rows

# ----------------------------
# Writer
# ----------------------------
def main():
    # Prepare output
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Create manager pool first
    manager_pool = build_manager_pool(TOTAL_ROWS)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

        batches = math.ceil(TOTAL_ROWS / CHUNK_SIZE)
        current = 1
        for b in range(batches):
            end = min(current + CHUNK_SIZE - 1, TOTAL_ROWS)
            rows = generate_rows(current, end, manager_pool)
            writer.writerows(rows)
            current = end + 1

    print(f"Done. Wrote {TOTAL_ROWS} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
