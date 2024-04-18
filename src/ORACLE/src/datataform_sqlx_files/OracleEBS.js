[
'OracleEbs-PER_ALL_ASSIGNMENTS_F',
'OracleEbs-PAY_COST_ALLOCATIONS_F',
'OracleEbs-PER_PERIODS_OF_SERVICE',
'OracleEbs-PER_PERIODS_OF_PLACEMENT',
'OracleEbs-TTEC_EMP_PROJ_ASG',
'OracleEbs-PER_BUSINESS_GROUPS',
'OracleEbs-PER_ALL_PEOPLE_F',
'OracleEbs-PER_JOBS',
'OracleEbs-PER_ASSIGNMENT_STATUS_TYPES',
'OracleEbs-HrLookups',
'OracleEbs-PER_PHONES',
'OracleEbs-GL_SETS_OF_BOOKS',
'OracleEbs-FND_FLEX_VALUES_VL',
'OracleEbs-PER_JOB_DEFINITIONS',
'OracleEbs-PER_PERSON_TYPES',
'OracleEbs-PAY_ALL_PAYROLLS_F',
'OracleEbs-HR_SOFT_CODING_KEYFLEX',
'OracleEbs-PER_GRADES',
'OracleEbs-PAY_COST_ALLOCATION_KEYFLEX',
'OracleEbs-HR_ALL_ORGANIZATION_UNITS_TL',
'OracleEbs-FND_FLEX_VALUES_TL',


]
.forEach(source => declare({
      schema: "OracleEBS",
      name: source
    })
  );