#'
#' @param oldToNewCohortId    A data.frame object with two columns. oldCohortId and newCohortId. Both should be integers.
#'                            The oldCohortId are the cohorts that are the input cohorts that need to be transformed.
#'                            The newCohortId are the cohortIds of the corresponding output after transformation.
#'                            If the oldCohortId = newCohortId then the data corresponding to oldCohortId 
#'                            will be replaced by the data from the newCohortId.
#' @param purgeConflicts      If there are conflicts in the target cohort table i.e. the target cohort table
#'                            already has records with newCohortId, do you want to purge and replace them 
#'                            with transformed. By default - it will not be replaced, and an error message is thrown.
