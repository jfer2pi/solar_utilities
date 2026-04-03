library(dplyr)
library(lubridate)
library(magrittr)
library(purrr)
library(stringr)
library(tibble)
library(cli)


# Monthly dataframe creation ---------------------------------------------

#' Build a month-by-month date range table
#'
#' Splits the interval \[start_time, end_time\] into one row per calendar month.
#' The first row's \code{start_time} is clamped to the supplied start date
#' (i.e. mid-month starts are respected), and the last row's \code{end_time}
#' is clamped to the supplied end date (i.e. mid-month ends are respected).
#' All other rows span the full calendar month.
#'
#' @param start_time Date or character coercible to Date. Inclusive start of the
#'   full period.
#' @param end_time   Date or character coercible to Date. Inclusive end of the
#'   full period.
#'
#' @return A tibble with two Date columns:
#'   \describe{
#'     \item{start_time}{First day of each month (or the supplied start date for
#'       the first row).}
#'     \item{end_time}{Last day of each month (or the supplied end date for the
#'       last row).}
#'   }
#'
#' @examples
#' create_monthly_df("2024-03-15", "2024-06-10")
#' # start_time   end_time
#' # 2024-03-15   2024-03-31
#' # 2024-04-01   2024-04-30
#' # 2024-05-01   2024-05-31
#' # 2024-06-01   2024-06-10

create_monthly_df <- function(start_time, end_time) {
  start_time <- as.Date(start_time)
  end_time   <- as.Date(end_time)

  # Sequence of first days for every calendar month in the range
  first_days <- seq(
    from = floor_date(start_time, "month"),
    to   = floor_date(end_time,   "month"),
    by   = "month"
  )

  # Default end of each month, clamped so the final row never exceeds end_time
  last_days <- pmin(ceiling_date(first_days, "month") - days(1), end_time)

  # Clamp the first row's start to the supplied start_time (mid-month support)
  first_days[1L] <- max(first_days[1L], start_time)

  tibble(start_time = first_days, end_time = last_days)
}


# Robust sensor mean -----------------------------------------------------

#' Iterative robust mean with outlier elimination
#'
#' Computes a mean from a vector of sensor readings, iteratively dropping the
#' single most deviant value until all remaining values fall within
#' \code{threshold} of the current mean, or until the sensor floor
#' (\code{min_vals}) or iteration ceiling (\code{max_iter}) is reached.
#'
#' The algorithm at each iteration:
#' \enumerate{
#'   \item Compute the mean \eqn{\mu} of the current value set.
#'   \item If \eqn{|\mu| < \text{min\_mean}} exit immediately (near-zero
#'     denominator guard).
#'   \item Compute the percentage error for each value:
#'     \eqn{e_i = |1 - v_i / \mu|}.
#'   \item If \eqn{\max(e_i) \leq \text{threshold}} the set has converged —
#'     exit.
#'   \item If more than \code{min_vals} values remain, drop the worst offender
#'     and repeat. Otherwise break immediately (cannot drop below floor).
#' }
#'
#' Invalid inputs (non-numeric, \code{NA}, or \eqn{\leq \text{min\_value}})
#' are removed before any iteration and counted in \code{n_invalid}.
#'
#' @param values    Numeric vector of sensor readings for one timestamp.
#' @param threshold Convergence threshold: maximum allowable percentage
#'   deviation from the mean (default 0.05 = 5\%).
#' @param max_iter  Hard upper bound on filtering iterations (default 5).
#' @param min_vals  Minimum number of sensors required to continue dropping;
#'   the algorithm stops dropping when this floor is reached (default 2).
#' @param min_value Hard lower bound on valid readings. Values at or below this
#'   level are treated as invalid and excluded before iteration (default
#'   \code{-Inf}, i.e. no lower bound).
#' @param min_mean  If the absolute value of the running mean falls below this
#'   threshold, convergence is assumed immediately. Prevents division by
#'   near-zero values (default 1).
#'
#' @return A one-row tibble with columns:
#'   \describe{
#'     \item{mean}{Robust mean of the retained values, or \code{NA} if no valid
#'       inputs were supplied.}
#'     \item{n_used}{Number of sensor readings used in the final mean.}
#'     \item{n_invalid}{Number of readings excluded as invalid before iteration
#'       (NA, non-numeric, or below \code{min_value}).}
#'     \item{n_dropped}{Number of valid readings dropped as outliers during
#'       iteration.}
#'     \item{iterations}{Number of iterations actually performed.}
#'   }

robust_sensor_mean <- function(values,
                               threshold = 0.05,
                               max_iter  = 5,
                               min_vals  = 2,
                               min_value = -Inf,
                               min_mean  = 1) {

  n_raw <- length(values)

  # Coerce to numeric and remove invalid entries (NA, non-numeric, <= min_value)
  values <- suppressWarnings(as.numeric(values))
  vals   <- values[!is.na(values) & values > min_value]

  n_valid   <- length(vals)
  n_invalid <- n_raw - n_valid

  # Nothing to average — return an NA row with full accounting
  if (n_valid == 0L) {
    return(tibble(
      mean       = NA_real_,
      n_used     = 0L,
      n_invalid  = n_invalid,
      n_dropped  = 0L,
      iterations = 0L
    ))
  }

  iter <- 0L

  for (i in seq_len(max_iter)) {
    iter <- i
    mu   <- mean(vals)

    # Guard against near-zero means that would make pct_err meaningless
    if (is.na(mu) || abs(mu) < min_mean) break

    pct_err <- abs(1 - vals / mu)
    max_err <- max(pct_err, na.rm = TRUE)

    # Converged: all values within threshold of the mean
    if (max_err <= threshold) break

    if (length(vals) > min_vals) {
      # Drop the single worst offender and iterate
      vals <- vals[-which.max(pct_err)]
    } else {
      # At the sensor floor — cannot drop further; accept the current mean
      break
    }
  }

  tibble(
    mean       = mean(vals),
    n_used     = length(vals),
    n_invalid  = n_invalid,
    n_dropped  = n_valid - length(vals),
    iterations = iter
  )
}


# Tracker exclusions -----------------------------------------------------

#' Apply per-tracker exclusion flags to sensor columns
#'
#' Joins a wide-format exclusion table onto \code{df} by \code{date_hour}, then
#' sets any sensor column to \code{NA} for rows where the corresponding tracker
#' exclusion flag is \code{TRUE}.
#'
#' The exclusion table (\code{excl_wide}) must contain one logical column per
#' tracker named \code{excl_p<N>} (e.g. \code{excl_p1}, \code{excl_p2}), where
#' \code{<N>} matches the tracker index extracted from the sensor column names
#' via the pattern \code{p\d+}.
#'
#' @param df        Data frame containing sensor columns and a \code{date_hour}
#'   join key.
#' @param excl_wide Wide-format exclusion table with a \code{date_hour} column
#'   and one \code{excl_p<N>} logical column per tracker.
#' @param pattern   Regex used to identify sensor columns in \code{df} that
#'   require exclusion checking (e.g. \code{"^irr_p"}, \code{"^poa_"}).
#'
#' @return \code{df} left-joined with \code{excl_wide}, with flagged sensor
#'   values replaced by \code{NA}. The \code{excl_*} columns are dropped before
#'   returning.

apply_tracker_exclusions <- function(df, excl_wide, pattern) {

  # Join exclusion flags onto the main data by timestamp
  df_joined   <- left_join(df, excl_wide, by = "date_hour")
  sensor_cols <- grep(pattern, names(df_joined), value = TRUE)

  for (col in sensor_cols) {
    # Derive the tracker index (e.g. "p1") from the sensor column name
    cab      <- str_extract(col, "p\\d+")
    excl_col <- paste0("excl_", cab)   # e.g. "excl_p1"

    if (excl_col %in% names(df_joined)) {
      # Replace with NA wherever the exclusion flag is TRUE
      df_joined[[col]] <- ifelse(df_joined[[excl_col]], NA_real_, df_joined[[col]])
    }
  }

  # Drop all temporary exclusion flag columns before returning
  select(df_joined, -starts_with("excl_"))
}


# Add robust mean --------------------------------------------------------

#' Add a robust mean and diagnostics for a group of sensor columns
#'
#' Selects all columns in \code{df} that match \code{pattern}, applies
#' \code{\link{robust_sensor_mean}} row-wise via \code{purrr::pmap}, and
#' appends the resulting diagnostic columns to the original data frame.
#'
#' Output column names follow the convention
#' \code{<output_prefix>mean}, \code{<output_prefix>n_used}, etc.
#' When \code{output_prefix} is \code{NULL} (default), a clean prefix is
#' derived automatically from \code{pattern} by stripping anchors (\code{^},
#' \code{$}) and any remaining regex metacharacters, then appending \code{_}
#' as a separator.
#'
#' @param df            A data frame or tibble.
#' @param pattern       Regex string matching the sensor column names to
#'   average (e.g. \code{"^irr_p"}, \code{"poa_"}).
#' @param threshold     Percentage error convergence threshold passed to
#'   \code{robust_sensor_mean} (default 0.05).
#' @param max_iter      Maximum iteration count passed to
#'   \code{robust_sensor_mean} (default 5).
#' @param min_vals      Minimum sensor floor passed to
#'   \code{robust_sensor_mean} (default 2).
#' @param output_prefix Character string prefix for the appended output
#'   columns. If \code{NULL}, derived automatically from \code{pattern}.
#'
#' @return \code{df} with five additional columns appended:
#'   \code{<prefix>mean}, \code{<prefix>n_used}, \code{<prefix>n_invalid},
#'   \code{<prefix>n_dropped}, \code{<prefix>iterations}.
#'
#' @examples
#' df |> add_robust_mean("^irr_p", output_prefix = "irr_")
#' # appends: irr_mean, irr_n_used, irr_n_invalid, irr_n_dropped, irr_iterations

add_robust_mean <- function(df,
                            pattern,
                            threshold     = 0.05,
                            max_iter      = 5,
                            min_vals      = 2,
                            output_prefix = NULL) {

  sensor_df <- select(df, matches(pattern))

  if (ncol(sensor_df) == 0L)
    cli_abort("No columns matched pattern {.val {pattern}}")

  # Derive a clean column prefix from the regex when none is explicitly supplied:
  #   1. Strip leading ^ and trailing $ anchors
  #   2. Remove remaining regex metacharacters ([, ], +, ?, etc.)
  #   3. Ensure a trailing _ separator
  if (is.null(output_prefix)) {
    output_prefix <- pattern |>
      gsub("^\\^|\\$$", "", x = _) |>
      gsub("[^[:alnum:]_.]", "", x = _)
    if (!endsWith(output_prefix, "_"))
      output_prefix <- paste0(output_prefix, "_")
  }

  # Apply robust_sensor_mean row-wise across matched sensor columns
  results <- pmap(
    sensor_df,
    \(...) robust_sensor_mean(
      c(...),
      threshold = threshold,
      max_iter  = max_iter,
      min_vals  = min_vals
    )
  ) |>
    list_rbind() |>
    rename_with(\(x) paste0(output_prefix, x))
  # → e.g. irr_mean, irr_n_used, irr_n_invalid, irr_n_dropped, irr_iterations

  bind_cols(df, results)
}


# Period exclusions (non-equi overlap join) ------------------------------

#' Apply time-period exclusions using an efficient overlap join
#'
#' Marks or removes rows of \code{df} whose timestamp falls within any of the
#' exclusion windows defined in \code{exclusions}.
#'
#' Internally the function uses a dplyr \code{>= 1.1.0} non-equi
#' \code{inner_join} with \code{join_by(between(...))} to find matching
#' windows. This avoids the O(n × m) cartesian product of the previous
#' \code{cross_join + filter} approach: only matching pairs are materialised,
#' so performance scales to large exclusion lists without memory pressure.
#'
#' When a timestamp falls inside more than one exclusion window, the window
#' with the earliest \code{start_date} is used as the canonical \code{reason}.
#'
#' @param df            Data frame to filter. Must contain the column named by
#'   \code{datetime_col}.
#' @param exclusions    Data frame of exclusion windows with (at minimum)
#'   columns \code{start_date}, \code{end_date} (same type as
#'   \code{datetime_col}), and \code{reason} (character).
#' @param datetime_col  Name of the timestamp column in \code{df} used for
#'   the overlap match (default \code{"date_hour"}).
#' @param keep_excluded Logical. If \code{FALSE} (default), excluded rows are
#'   dropped and the \code{reason} column is not returned. If \code{TRUE}, all
#'   rows are returned with a \code{reason} column: \code{NA} for non-excluded
#'   rows, and the matched reason string for excluded rows.
#'
#' @return A tibble. If \code{keep_excluded = FALSE}, only non-excluded rows
#'   are returned (no \code{reason} column). If \code{keep_excluded = TRUE},
#'   all original rows are returned with a \code{reason} column appended.
#'
#' @note Requires dplyr >= 1.1.0 for \code{join_by(between(...))}.

apply_period_exclusions <- function(df,
                                    exclusions,
                                    datetime_col  = "date_hour",
                                    keep_excluded = FALSE) {

  # Attach a row key so we can re-join results back to the original data frame
  df_keyed <- mutate(df, .row_id = row_number())

  # Minimal table: just the row key and the timestamp to match against
  ts_tbl <- select(df_keyed, .row_id, .ts = all_of(datetime_col))

  # Non-equi overlap join: for each .ts, find all exclusion windows [start_date,
  # end_date] that contain it.
  # multiple = "all"  — suppress the expected "multiple matches" warning; a
  #                     single timestamp can legitimately fall in several windows.
  matched <- inner_join(
    ts_tbl,
    exclusions,
    by       = join_by(between(.ts, start_date, end_date)),
    multiple = "all"
  ) |>
    # When multiple windows match the same row, keep the earliest-starting one
    # as the canonical reason (first window wins)
    slice_min(order_by = start_date, by = .row_id, with_ties = FALSE) |>
    select(.row_id, reason)

  # Re-attach the reason flag to the full data frame, then clean up the row key
  result <- left_join(df_keyed, matched, by = ".row_id") |>
    select(-.row_id)

  # Either return all rows (with reason column) or drop excluded rows silently
  if (keep_excluded) result else filter(result, is.na(reason))
}
