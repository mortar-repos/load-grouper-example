# Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  You create your project using the Mortar Development Framework, deploy code using the Git revision control system, and Mortar does the rest.

# Getting Started

This Mortar project includes some example code to show how you could implement an iterative algorithm using Mortar and Pig.  To get started:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1.  Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:mortardata/load-grouper-example.git
        cd load-grouper-example
        mortar register load-grouper-example

Once everything is set up you can run this example by doing:
        mortar run iterate-example

# What's Inside

There are a couple of different pieces to this example project:

### LoadGrouper

The load-grouper directory contains a small Java project for building a Pig loader that will operate over an input split and emit only one row for that split.  This can be useful in a number of machine learning algorithm such as [Alternating Direction Method of Multipliers](http://www.stanford.edu/~boyd/papers/admm_distr_stats.html).

This LoadGrouper implementation simply counts the number of data rows in the split and emits a single tuple with the split file name, the split index, and that count.

To build the LoadGrouper simply go to the load-grouper directory and run: 
    mvn install 

### Control Script

The file ./controlscripts/iterate-example.py is the top level script that we're going to run in Mortar.  Using [Embedded Pig](http://pig.apache.org/docs/r0.9.1/cont.html) this script runs a standard pig script multiple times.  In this example we're not trying to calculate anything meaningful but instead are demonstrating a few different features:

1. Using iteration to run the same pig script multiple times.
1. Passing a parameter (ITERATION\_NUM) calculated in the control script into the pig script for use in the pig job.
1. Retrieving the results of an alias from the pig job to be used in a calculation in the control script.
1. Demonstrating how print statements in the control script will show up in the Pig logs on the Mortar job details page.

### Pig Script

The file ./pigscripts/avg\_songs\_per\_split\_counter.pig is a standard pig script that uses the LoadGrouper UDF above to load a number of files from the public Millionsong Dataset and count how many songs end up being in each input split.  After that we average that count across all splits and save that out to S3.

Notice that while this script can be called from our jython control script its just a standard pig script and as such we could run it using:
        mortar run avg_songs_per_split_counter -p ITERATION_NUM=0

