# P2-6 — CloudWatch dashboard for the live StreamGuard observability signals.
#
# Panels: the 4 custom metrics the consumer publishes (StreamGuard/Inference)
# plus one alarm-status panel covering all 3 alarms. Dashboard widgets are free
# (no per-widget charge), so this adds ~$0.00/mo on top of the alarm budget.
#
# The metric widgets use Sum with the same periods as the alarms (300 s for the
# workload counters, 60 s for the heartbeat) so what you see is what the alarms
# evaluate.

resource "aws_cloudwatch_dashboard" "streamguard" {
  dashboard_name = "StreamGuard-Live"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [["StreamGuard/Inference", "ScoredTransactions"]]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          title   = "ScoredTransactions (5-min sum)"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [["StreamGuard/Inference", "FraudAlertCount"]]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          title   = "FraudAlertCount (5-min sum)"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [["StreamGuard/Inference", "ScorerErrorCount"]]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          title   = "ScorerErrorCount (5-min sum)"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [["StreamGuard/Inference", "ConsumerHeartbeat"]]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 60
          stat    = "Sum"
          title   = "ConsumerHeartbeat (per-minute) — flatline = dead consumer"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 24
        height = 4
        properties = {
          view    = "singleValue"
          title   = "Alarm status"
          region  = var.region
          period  = 300
          stat    = "Average"
          metrics = [["AWS/CloudWatch", "AlarmState", { "stat" = "Average" }]]
          alarms = [
            aws_cloudwatch_metric_alarm.consumer_error_alarm.arn,
            aws_cloudwatch_metric_alarm.fraud_spike_alarm.arn,
            aws_cloudwatch_metric_alarm.consumer_heartbeat_alarm.arn,
          ]
        }
      },
    ]
  })
}
