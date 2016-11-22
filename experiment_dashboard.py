#!/home/hadoop/anaconda2/bin/ipython
import pip
pip.main(['install', '--upgrade', 'plotly'])

import sys
import math
import time
import boto3
import plotly
import argparse
import pandas as pd
import datetime as dt
import requests, json
from scipy import stats
import plotly.plotly as py
from datetime import datetime
import plotly.graph_objs as go
from pyspark.sql import SQLContext
import statsmodels.stats.power as smp
from boto3.s3.transfer import S3Transfer
from pyspark import SparkContext, SparkConf
from plotly.tools import FigureFactory as FF
from pyspark.sql.functions import col, countDistinct, unix_timestamp

ALPHA_ERROR = 0.05
EVENTS = ['CLICK', 'BLOCK', 'DELETE', 'SEARCH', 'BOOKMARK_ADD']

def get_variant(variant):
  if (variant == 'exp'):
    return col("experiment_id") == experiment_id
  else:
    return (col("experiment_id") == 'n/a') | (col("experiment_id").isNull())

def parse_arguments(argv):
  print "Parsing command line arguments."
  parser = argparse.ArgumentParser(description='Generate a plotly dashboard for a given experiment.')
  parser.add_argument('exp_id', type=str, help='the experiment ID string (e.g. exp-004-local-metadata)')
  parser.add_argument('start_date', type=int, help='experiment start date - unix timestamp')
  parser.add_argument('end_date', type=int, help='experiment end date - unix timestamp')
  parser.add_argument('--versions', help='the addon versions where the experiment is enabled', nargs='+')
  args = parser.parse_args()
  print args.versions

  if args.start_date > args.end_date:
    raise Exception('start date should be before end date')

  return args.exp_id, datetime.fromtimestamp(args.start_date), datetime.fromtimestamp(args.end_date), args.versions

def get_months(start, end):
  months = []
  num_months = end.month - start.month + 1

  for i in xrange(num_months):
    months.append((start + dt.timedelta(days=i)).strftime("%m"))

  return months

def create_as_tables(months):
  print "Creating session and event tables."
  stats_table = sc.emptyRDD()
  events_table = sc.emptyRDD()

  for month in months:
    statsMonth = sc.textFile('s3://net-mozaws-prod-us-west-2-pipeline-analysis/tiles/activity_stream_stats_daily/2016/' + month + '/*/*.gz').map(lambda row:row.split('|'))
    eventsMonth = sc.textFile('s3://net-mozaws-prod-us-west-2-pipeline-analysis/tiles/activity_stream_events_daily/2016/' + month + '/*/*.gz').map(lambda row:row.split('|'))
    stats_table = stats_table.union(statsMonth)
    events_table = events_table.union(eventsMonth)

  stats_df = stats_table.toDF(["client_id", "tab_id", "load_reason", "unload_reason",
                               "max_scroll_depth", "load_latency", "total_bookmarks",
                               "total_history_size", "session_duration", "receive_at",
                               "locale", "country_code", "os", "browser", "version",
                               "device", "date", "addon_version", "page", "session_id", "experiment_id"])

  events_df = events_table.toDF(["client_id", "tab_id", "source", "action_position",
                                 "event", "receive_at", "date", "locale", "country_code",
                                 "os", "browser", "version", "device", "addon_version",
                                 "page", "session_id", "experiment_id", "recommendation_url", "recommender_type",
                                 "highlight_type", "share_provider", "metadata_source"])

  metric_table = [['Metric', 'Alpha Error', 'Power', 'P-value (ttest)']]

  return stats_table, events_table, metric_table

def nav_only_sessions(variant):
  return stats_df \
    .where(stats_df.session_id != 'n/a') \
    .where(stats_df.session_id.isNotNull()) \
    .where(stats_df.addon_version.isin(addon_versions)) \
    .where(stats_df.unload_reason == 'navigation') \
    .where(get_variant(variant)) \
    .join(events_df, stats_df.session_id == events_df.session_id, 'outer') \
    .select(stats_df.date, stats_df.session_id, stats_df.experiment_id, stats_df.addon_version, stats_df.unload_reason) \
    .groupBy("date", "session_id") \
    .count() \
    .where(col("count") <= 1) \
    .groupBy("date").count() \
    .orderBy("date")

def compute_power_and_p_value(control_rate, exp_rate):
  control_data = control_rate.select("control_ratio").collect()
  exp_data = exp_rate.select("exp_ratio").collect()
  
  control_count = float(control_rate.count())
  exp_count = float(exp_rate.count())
  control_rate_stats = control_rate.describe()
  exp_rate_stats = exp_rate.describe()
  control_rate_stats.show()
  exp_rate_stats.show()

  stddev_control_rate = float(control_rate_stats.select("control_ratio").collect()[2].control_ratio)
  stddev_exp_rate = float(exp_rate_stats.select("exp_ratio").collect()[2].exp_ratio)

  mean_control_rate = float(control_rate_stats.select("control_ratio").collect()[1].control_ratio)
  mean_exp_rate = float(exp_rate_stats.select("exp_ratio").collect()[1].exp_ratio)
  
  percent_diff = abs(mean_control_rate - mean_exp_rate) / mean_control_rate
  pooled_stddev = math.sqrt(((pow(stddev_control_rate, 2) * (control_count - 1)) + \
                  (pow(stddev_exp_rate, 2) * (exp_count - 1))) / \
                  ((control_count - 1) + (exp_count - 1)))
  effect_size = (percent_diff * float(mean_control_rate)) / float(pooled_stddev)
  power = smp.TTestIndPower().solve_power(effect_size,
                                        nobs1=control_count,
                                        ratio=exp_count / control_count,
                                        alpha=ALPHA_ERROR, alternative='two-sided')
  p_val = stats.ttest_ind(control_data, exp_data, equal_var = False).pvalue
  return power, p_val


if __name__ == '__main__':
  conf = SparkConf().setAppName('dashboard')
  sc = SparkContext(conf=conf)
  sqlContext = SQLContext(sc)

  experiment_id, start_date, end_date, versions = parse_arguments(sys.argv[1:])
  months = get_months(start_date, end_date)
  stats_table, events_table, metric_table = create_as_tables(months)

  sc.stop()

