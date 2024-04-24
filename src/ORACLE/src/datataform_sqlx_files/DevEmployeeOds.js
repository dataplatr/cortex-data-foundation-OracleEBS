  [
'OracleEbs-PER_JOBS',
'OracleEbs-PER_ALL_PEOPLE_F',
'OracleEbs-GL_DAILY_RATES'
]
  .forEach(source => declare({
      schema: "DevEmployeeOds",
      name: source
    })
  );