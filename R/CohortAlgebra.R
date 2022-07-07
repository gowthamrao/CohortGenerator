# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Era-fy cohort(s)
#'
#' @description
#' Given a table with cohort_definition_id, subject_id, cohort_start_date,
#' cohort_end_date execute era logic. This will delete and replace the
#' original rows with the cohort_definition_id(s). edit privileges
#' to the cohort table is required.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds           An array of cohort Ids in the cohort table to apply cohort era logic.
#' 
#' @param eraconstructorpad   Optional value to pad cohort era construction logic. Default = 0. i.e. no padding.
#'
#' @return
#' NULL
#'
#' @export
eraFyCohort <- function(connectionDetails = NULL,
                        connection = NULL,
                        cohortDatabaseSchema,
                        cohortTable = "cohort",
                        cohortIds,
                        eraconstructorpad = 0) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = cohortIds,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)
  
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sqlCopyCohort <- "
                  DROP TABLE IF EXISTS #cohort_rows;
                  SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
                  INTO #cohort_rows
                  FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
                  WHERE cohort_definition_id IN (@cohort_ids);"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCopyCohort,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  
  sqlEraFy <- " DROP TABLE IF EXISTS #final_cohort;
                with cteEndDates (cohort_definition_id, subject_id, cohort_end_date) AS -- the magic
                (
                	SELECT
                	  cohort_definition_id
                		, subject_id
                		, DATEADD(day,-1 * @eraconstructorpad, event_date)  as cohort_end_date
                	FROM
                	(
                		SELECT
                		  cohort_definition_id
                			, subject_id
                			, event_date
                			, event_type
                			, MAX(start_ordinal) OVER (PARTITION BY cohort_definition_id, subject_id
                			                            ORDER BY event_date, event_type, start_ordinal ROWS UNBOUNDED PRECEDING) AS start_ordinal
                			, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id
                			                      ORDER BY event_date, event_type, start_ordinal) AS overall_ord
                		FROM
                		(
                			SELECT
                			  cohort_definition_id
                				, subject_id
                				, cohort_start_date AS event_date
                				, -1 AS event_type
                				, ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id ORDER BY cohort_start_date) AS start_ordinal
                			FROM #cohort_rows

                			UNION ALL


                			SELECT
                			  cohort_definition_id
                				, subject_id
                				, DATEADD(day,@eraconstructorpad,cohort_end_date) as cohort_end_date
                				, 1 AS event_type
                				, NULL
                			FROM #cohort_rows
                		) RAWDATA
                	) e
                	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
                ),
                cteEnds (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) AS
                (
                	SELECT
                	  c. cohort_definition_id
                		, c.subject_id
                		, c.cohort_start_date
                		, MIN(e.cohort_end_date) AS cohort_end_date
                	FROM #cohort_rows c
                	JOIN cteEndDates e ON c.cohort_definition_id = e.cohort_definition_id AND
                	                      c.subject_id = e.subject_id AND
                	                      e.cohort_end_date >= c.cohort_start_date
                	GROUP BY c.cohort_definition_id, c.subject_id, c.cohort_start_date
                )
                select cohort_definition_id, subject_id, min(cohort_start_date) as cohort_start_date, cohort_end_date
                into #final_cohort
                from cteEnds
                group by cohort_definition_id, subject_id, cohort_end_date
                ;
  "
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlEraFy,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    eraconstructorpad = eraconstructorpad
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DELETE
            FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            WHERE cohort_definition_id = @cohort_ids;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM #final_cohort
            WHERE cohort_definition_id = @cohort_ids;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema, 
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS #cohort_rows;
            DROP TABLE IF EXISTS #final_cohort;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}



#' union cohort
#'
#' @description
#' Given cohortIds of instantiated cohorts in a cohort table,
#' create a new derived cohort in the same table with a new cohort_definition_id
#' that is derived by performing a union of the given cohortIds and ensuring
#' that they conform to cohort era rules by applying era-fy logic.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds            An array of one or more cohort ids
#'
#' @param newCohortId         The cohort id of the new derived cohort. This should not already
#'                            exist in the cohort table.
#'
#' @return
#' NULL
#'
#' @export
unionCohort <- function(connectionDetails = NULL,
                        connection = NULL,
                        cohortDatabaseSchema,
                        cohortTable = "cohort",
                        cohortIds,
                        newCohortId) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = cohortIds,
    min.len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    x = newCohortId,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
    min.chars = 1,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = cohortTable,
    min.chars = 1,
    len = 1,
    null.ok = FALSE,
    add = errorMessages
  )
  checkmate::reportAssertions(collection = errorMessages)
  
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  cohortIdsInCohortTable <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT cohort_definition_id cohort_definition_id
             FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table};",
      snakeCaseToCamelCase = TRUE,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable
    )
  
  if (length(intersect(cohortIdsInCohortTable$cohortDefinitionId, newCohortId)) == 1) {
    stop(
      paste0(
        "There is already a cohort with the newCohortId ",
        newCohortId,
        " in the cohort table ",
        cohortTable,
        "."
      )
    )
  }
  
  sqlUnionCohort <- "
                  DROP TABLE IF EXISTS #cohort_union;
                  SELECT @new_cohort_id cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
                  INTO #cohort_union
                  FROM {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
                  WHERE cohort_definition_id IN (@cohort_ids);"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlUnionCohort,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    new_cohort_id = newCohortId
  )
  
  eraFyCohort(
    connection = connection,
    cohortDatabaseSchema = NULL,
    cohortTable = "#cohort_union",
    cohortId = newCohortId
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " INSERT INTO {@cohort_database_schema != ''} ? {@cohort_database_schema.@cohort_table} : {@cohort_table}
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM #cohort_union
            WHERE cohort_definition_id = @new_cohort_id;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    new_cohort_id = newCohortId,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS #cohort_union;",
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}
