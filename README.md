# ccy
Running jupyter: jupyter notebook
Environment setup : . ./.zprofile

## Testing
~testQuick for incremental tests continuously

## Subproject
lazy val hello = 
   (project in file (".")
      .settings(...))

lazy val helloCore = 
   (project in file ("core")
      .settings(...))
	  
	  
## dependsOn
lazy val hello = 
   (project in file (".")
	  .aggregate(helloCore)
	  .dependsOn(helloCore)
      .settings(...))

lazy val helloCore = 
   (project in file ("core")
      .settings(...))

## Tasks
TaskKey define tasks - compile, packaage.
re-run each time

Setting Keys run only once.

// to list keysd
sbt settings -v 
sbt tasks
sbt help <task

## Scope
tuple of (project/subproject x configuration x task)



# Hive 
[Hive Setup|https://cwiki.apache.org/confluence/display/Hive//GettingStarted]

## Data Sources
1. yahoo finance - use yfinance with pip
2. quandl
3. alphaadvantage

# Yahoo Finance
[API|https://javadoc.io/doc/de.sfuhrm/YahooFinanceAPI/latest/yahoofinance/package-summary.html]
[Github|https://github.com/sfuhrm/yahoofinance-api]
