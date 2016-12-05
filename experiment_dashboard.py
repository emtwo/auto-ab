#!/home/hadoop/anaconda2/bin/ipython
import pip
pip.main(['install', '--upgrade', 'plotly'])
pip.main(['install', '--upgrade', 'py4j'])

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

class ExperimentDashboard:
  ALPHA_ERROR = 0.05
  EVENTS = ['CLICK', 'BLOCK', 'DELETE', 'SEARCH', 'BOOKMARK_ADD']

  def __init__(self, args):
    self.graphs = []
    self.sc = SparkContext(conf=SparkConf().setAppName('dashboard'))
    self.sqlContext = SQLContext(self.sc)
    self.parse_arguments(args)
    self.months = self.get_months()
    self.create_as_tables()
    self.set_session_counts()
    self.create_daily_event_graphs()
    self.create_nav_only_graphs()

  def set_session_counts(self):
    self.control_session_counts_per_day = self.distinct_sessions_or_events(self.stats_df, "control")
    self.exp_session_counts_per_day = self.distinct_sessions_or_events(self.stats_df, "exp")

  def uninit(self):
    self.sc.stop()

  def get_variant(self, variant):
    if (variant == 'exp'):
      return col("experiment_id") == self.experiment_id
    else:
      return (col("experiment_id") == 'n/a') | (col("experiment_id").isNull())

  def parse_arguments(self, argv):
    print "Parsing command line arguments."
    parser = argparse.ArgumentParser(description='Generate a plotly dashboard for a given experiment.')
    parser.add_argument('exp_id', type=str, help='the experiment ID string (e.g. exp-004-local-metadata)')
    parser.add_argument('start_date', type=int, help='experiment start date - unix timestamp')
    parser.add_argument('end_date', type=int, help='experiment end date - unix timestamp')
    parser.add_argument('--versions', help='the addon versions where the experiment is enabled', nargs='+')
    args = parser.parse_args()

    if args.start_date > args.end_date:
      raise Exception('start date should be before end date')

    self.experiment_id = args.exp_id
    self.start_date = datetime.fromtimestamp(args.start_date)
    self.end_date = datetime.fromtimestamp(args.end_date)
    self.versions = args.versions

  def distinct_sessions_or_events(self, table, variant, event=None):
    req_fields = table
    if (event):
      req_fields = req_fields \
        .select("date", "client_id", "experiment_id", "addon_version", "session_id", "event") \
        .where(col("event").isin(event))
    else:
      req_fields = req_fields \
        .select("date", "client_id", "experiment_id", "addon_version", "session_id") \

    return req_fields \
      .where(col("session_id") != 'n/a') \
      .where(col("session_id").isNotNull()) \
      .where(col("addon_version").isin(self.versions)) \
      .where(self.get_variant(variant)) \
      .groupBy("date") \
      .agg(countDistinct("session_id").alias(variant + "_" + ("event" if event else "session") + "_count")) \
      .orderBy("date")

  def get_months(self):
    months = []
    num_months = self.end_date.month - self.start_date.month + 1

    for i in xrange(num_months):
      months.append((self.start_date + dt.timedelta(days=(i*31))).strftime("%m"))

    return months

  def create_as_tables(self):
    print "Creating session and event tables."
    stats_table = self.sc.emptyRDD()
    events_table = self.sc.emptyRDD()

    for month in self.months:
      statsMonth = self.sc.textFile('s3://net-mozaws-prod-us-west-2-pipeline-analysis/tiles/activity_stream_stats_daily/2016/' + month + '/*/*.gz').map(lambda row:row.split('|'))
      eventsMonth = self.sc.textFile('s3://net-mozaws-prod-us-west-2-pipeline-analysis/tiles/activity_stream_events_daily/2016/' + month + '/*/*.gz').map(lambda row:row.split('|'))
      stats_table = stats_table.union(statsMonth)
      events_table = events_table.union(eventsMonth)

    self.stats_df = stats_table.toDF(["client_id", "tab_id", "load_reason", "unload_reason",
                                      "max_scroll_depth", "load_latency", "total_bookmarks",
                                      "total_history_size", "session_duration", "receive_at",
                                      "locale", "country_code", "os", "browser", "version",
                                      "device", "date", "addon_version", "page", "session_id", "experiment_id"])

    self.events_df = events_table.toDF(["client_id", "tab_id", "source", "action_position",
                                        "event", "receive_at", "date", "locale", "country_code",
                                        "os", "browser", "version", "device", "addon_version",
                                        "page", "session_id", "experiment_id", "recommendation_url", "recommender_type",
                                        "highlight_type", "share_provider", "metadata_source"])

    self.metric_table = [['Metric', 'Alpha Error', 'Power', 'P-value (ttest)']]

  def nav_only_sessions(self, variant):
    return self.stats_df \
      .where(self.stats_df.session_id != 'n/a') \
      .where(self.stats_df.session_id.isNotNull()) \
      .where(self.stats_df.addon_version.isin(self.versions)) \
      .where(self.stats_df.unload_reason == 'navigation') \
      .where(self.get_variant(variant)) \
      .join(self.events_df, self.stats_df.session_id == self.events_df.session_id, 'outer') \
      .select(self.stats_df.date, self.stats_df.session_id, self.stats_df.experiment_id, self.stats_df.addon_version, self.stats_df.unload_reason) \
      .groupBy("date", "session_id") \
      .count() \
      .where(col("count") <= 1) \
      .groupBy("date").count() \
      .select("date", col("count").alias(variant + "_nav_count")) \
      .orderBy("date")

  def get_event_rate(self, variant, event, session_counts_per_day):
    clicks_by_day = self.distinct_sessions_or_events(self.events_df, variant, event)
    return session_counts_per_day \
      .join(clicks_by_day, clicks_by_day.date == session_counts_per_day.date) \
      .drop(session_counts_per_day.date) \
      .select("date", (col(variant + "_event_count") / col(variant + "_session_count") * 100).alias(variant + "_ratio")) \
      .orderBy("date")

  def compute_power_and_p_value(self, control_rate, exp_rate):
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
                                          alpha=self.ALPHA_ERROR, alternative='two-sided')
    p_val = stats.ttest_ind(control_data, exp_data, equal_var = False).pvalue
    return power, p_val

  def create_graph(self, exp_rate, control_rate, measurement,
                  x_axis=["date","date"],
                  y_axis=["exp_ratio", "control_ratio"],
                  x_label="Date",
                  y_label=None):

    y_label = y_label if y_label else measurement.lower() + "s / sessions * 100"

    # Create traces
    trace0 = go.Scatter(
        x = exp_rate.select(x_axis[0]).collect(),
        y = exp_rate.select(y_axis[0]).collect(),
        mode = 'lines',
        name = 'experiment'
    )
    trace1 = go.Scatter(
        x = control_rate.select(x_axis[1]).collect(),
        y = control_rate.select(y_axis[1]).collect(),
        mode = 'lines',
        name = 'control'
    )

    layout = dict(title = 'Experiment vs. Control ' + measurement.lower() + " Rate" ,
                  xaxis = dict(title=x_label),
                  yaxis = dict(title=y_label))

    data = [trace0, trace1]
    return dict(data=data, layout=layout)

  def create_daily_event_graphs(self):
    for event in self.EVENTS:
      print "Processing event: " + event
      control_event_rate = self.get_event_rate("control", event, self.control_session_counts_per_day)
      exp_event_rate = self.get_event_rate("exp", event, self.exp_session_counts_per_day)

      power, p_val = self.compute_power_and_p_value(control_event_rate, exp_event_rate)
      self.metric_table.append([event + ' Ratio', self.ALPHA_ERROR, power, p_val])
  
      fig = self.create_graph(exp_event_rate, control_event_rate, event)
      self.graphs.append(py.plot(fig, filename=self.experiment_id + "_" + event, auto_open=False))

  def create_nav_only_graphs(self):
    print "Computing nav-only graph"
    control_nav_only_rate = self.get_nav_only_rate("control", self.control_session_counts_per_day)
    exp_nav_only_rate = self.get_nav_only_rate("exp", self.exp_session_counts_per_day)

    graph_type = "navigation_only"
    power, p_val = self.compute_power_and_p_value(control_nav_only_rate, exp_nav_only_rate)
    self.metric_table.append([graph_type + ' Ratio', self.ALPHA_ERROR, power, p_val])
    fig = self.create_graph(exp_nav_only_rate, control_nav_only_rate, graph_type)
    self.graphs.append(py.plot(fig, filename=self.experiment_id + "_" + graph_type, auto_open=False))

  def print_graphs(self):
    print self.graphs

  def get_nav_only_rate(self, variant, session_counts_per_day):
    nav_only_sessions_by_day = self.nav_only_sessions(variant)
    return session_counts_per_day \
      .join(nav_only_sessions_by_day, nav_only_sessions_by_day.date == session_counts_per_day.date) \
      .drop(session_counts_per_day.date) \
      .select("date", (col(variant + "_nav_count") / col(variant + "_session_count") * 100).alias(variant + "_ratio")) \
      .orderBy("date")

  def get_event_rate(self, variant, event, session_counts_per_day):
    clicks_by_day = self.distinct_sessions_or_events(self.events_df, variant, event)
    return session_counts_per_day \
      .join(clicks_by_day, clicks_by_day.date == session_counts_per_day.date) \
      .drop(session_counts_per_day.date) \
      .select("date", (col(variant + "_event_count") / col(variant + "_session_count") * 100).alias(variant + "_ratio")) \
      .orderBy("date")


if __name__ == '__main__':
  dash = ExperimentDashboard(sys.argv[1:])
  dash.print_graphs()
  dash.uninit()
