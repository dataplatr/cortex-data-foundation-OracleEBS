[
    'Date-dimCalendar',
    'Finance-AccountPrimaryHierarchy',
    'Finance-ClientPrimaryHierarchy',
    'Finance-ClientSegmentsHierarchy',
    ]
      .forEach(source => declare({
          schema: "${edw_dataset}",
          name: source
        })
      );