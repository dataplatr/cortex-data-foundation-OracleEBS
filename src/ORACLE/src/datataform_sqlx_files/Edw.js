[
'Hyperion-ClientsByBusinessSegment',
'Client-dimClient',
'Finance-ClientHierarchy',
'Finance-ClientPortfolioHierarchy',
'Client-dimClientProject',
'Location-vGlLocGeoLocXref',
'Location-GLLocationExpanded',
'Client-dimClientChannel',
'Service-dimChannel',
'Date-dimCalendar',
'Production-Attrition',
'Finance-LocationHierarchy',
'Location-dimCountry',
`Location-dimLocation`,
`Finance-DepartmentHierarchy`,
`OvProdO-EmpPosAssgsHst`,
`Employee-Person`,
`Employee-DepartmentHierarchy`,
`Employee-FunctionHierarchy`,
`Client-ClientSegment`,
'Service-dimServiceExpanded',
'System-dimSystem',
'Service-dimRoutingDirection',
'Activity-dimInteractionType',
'Service-dimServiceFunction',
'Service-dimServiceFunctionType',
'Service-dimServiceAttribute',
'Location-dimTimeZone',
'Client-dimClientType',
'Client-dimClientOffer',
'Client-dimOffer',
'Location-dimLocationType',
'Employee-PersonGUID',
'Location-dimRegion','RA_CUST_TRX_LINE_GL_DIST_ALL',
'Finance-AccountPrimaryHierarchy',
'Finance-ClientPrimaryHierarchy',
'Finance-ClientSegmentsHierarchy',
'Employee-PersonLogonId',
'Activity-dimActivityFactColXref',
'System-dimSystemType',
'Employee-PersonAssignment_COPY',
'Ewfm-dimScheduleState',
'Ewfm-dimStateSuperStateMapping',
'Ewfm-dimSuperState',
'Ewfm-dimPerspective',
'Ewfm-dimExternalSchedule',
'Ewfm-dimExternalScheduleStateXref',
'TLAssist-QAEvaluation',
'TLAssist-OvertimeScheduled',
'TLAssist-OvertimeWorked',
'TLAssist-KronosLILO',
'TLAssist-vActionItemSubject',
'TLAssist-ActionItem',
'TLAssist-EmpowerScorecardReview',
'OvProdO-OvPayCds',
'TLAssist-vEmployee',
'TLAssist-vProject',
'TLAssist-vLocation',
'TLAssist-vJob',
'Production-Attrition_CC_IOR',
'Location-dimAddress'
]
  .forEach(source => declare({
      schema: "Edw",
      name: source
    })
  );