#!/usr/bin/env python

"""Test Failures

Update etc/test_lifecycle.yml to tag unreliable tests based on historic failure rates.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import datetime
import optparse
import os.path
import subprocess
import sys
import textwrap
import warnings

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildscripts import resmokelib
from buildscripts.resmokelib.utils import globstar
from buildscripts import test_failures as tf
from buildscripts.ciconfig import evergreen as ci_evergreen
from buildscripts.ciconfig import tags as ci_tags


if sys.version_info[0] == 2:
    _NUMBER_TYPES = (int, long, float)
else:
    _NUMBER_TYPES = (int, float)


Rates = collections.namedtuple("Rates", ["acceptable", "unacceptable"])


Config = collections.namedtuple("Config", [
    "test_fail_rates",
    "task_fail_rates",
    "variant_fail_rates",
    "distro_fail_rates",
    "reliable_min_runs",
    "reliable_time_period",
    "unreliable_min_runs",
    "unreliable_time_period",
])


DEFAULT_CONFIG = Config(
    test_fail_rates=Rates(acceptable=0.1, unacceptable=0.3),
    task_fail_rates=Rates(acceptable=0.1, unacceptable=0.3),
    variant_fail_rates=Rates(acceptable=0.2, unacceptable=0.4),
    distro_fail_rates=Rates(acceptable=0.2, unacceptable=0.4),
    reliable_min_runs=5,
    reliable_time_period=datetime.timedelta(weeks=1),
    unreliable_min_runs=20,
    unreliable_time_period=datetime.timedelta(weeks=4))


DEFAULT_PROJECT = "mongodb-mongo-master"


def write_yaml_file(yaml_file, lifecycle):
    """Writes the lifecycle object to yaml_file."""

    comment = (
        "This file was generated by {} and shouldn't be edited by hand. It was generated against"
        " commit {} with the following invocation: {}."
    ).format(sys.argv[0], callo(["git", "rev-parse", "HEAD"]).rstrip(), " ".join(sys.argv))

    lifecycle.write_file(yaml_file, comment)


def get_suite_tasks_membership(evg_conf):
    """Return a dictionary with keys of all suites and list of associated tasks."""
    suite_membership = collections.defaultdict(list)
    for task in evg_conf.tasks:
        suite = task.resmoke_suite
        if suite:
            suite_membership[suite].append(task.name)
    return suite_membership


def get_test_tasks_membership(evg_conf):
    """Return a dictionary with keys of all tests and list of associated tasks."""
    test_suites_membership = resmokelib.parser.create_test_membership_map(test_kind="js_test")
    suite_tasks_membership = get_suite_tasks_membership(evg_conf)
    test_tasks_membership = collections.defaultdict(list)
    for test in test_suites_membership.keys():
        for suite in test_suites_membership[test]:
            test_tasks_membership[test].extend(suite_tasks_membership[suite])
    return test_tasks_membership


def get_tests_from_tasks(tasks, test_tasks_membership):
    """Return a list of tests from list of specified tasks."""
    tests = []
    tasks_set = set(tasks)
    for test in test_tasks_membership.keys():
        if not tasks_set.isdisjoint(test_tasks_membership[test]):
            tests.append(test)
    return tests


def create_test_groups(tests):
    """Return groups of tests by their directory, i.e., jstests/core."""
    test_groups = collections.defaultdict(list)
    for test in tests:
        test_split = test.split("/")
        # If the test does not have a directory, then ignore it.
        if len(test_split) <= 1:
            continue
        test_dir = test_split[1]
        test_groups[test_dir].append(test)
    return test_groups


def create_batch_groups(test_groups, batch_size):
    """Return batch groups list of test_groups."""
    batch_groups = []
    for test_group_name in test_groups:
        test_group = test_groups[test_group_name]
        while test_group:
            batch_groups.append(test_group[:batch_size])
            test_group = test_group[batch_size:]
    return batch_groups


def callo(args):
    """Call a program, and capture its output."""
    return subprocess.check_output(args)


def git_commit_range_since(since):
    """Returns first and last commit in 'since' period specified.

    Specify 'since' as any acceptable period for git log --since.
    The period can be specified as '4.weeks' or '3.days'.
    """
    git_command = "git log --since={} --pretty=format:%H".format(since)
    commits = callo(git_command.split()).split("\n")
    return commits[-1], commits[0]


def git_commit_prior(revision):
    """Returns commit revision prior to one specified."""
    git_format = "git log -2 {revision} --pretty=format:%H"
    git_command = git_format.format(revision=revision)
    commits = callo(git_command.split()).split("\n")
    return commits[-1]


def unreliable_test(test_fr, unacceptable_fr, test_runs, min_run):
    """Check for an unreliable test.

    A test should be added to the set of tests believed not to run reliably when it has more
    than min_run executions with a failure percentage greater than unacceptable_fr.
    """
    return test_runs >= min_run and test_fr >= unacceptable_fr


def reliable_test(test_fr, acceptable_fr, test_runs, min_run):
    """Check for a reliable test.

    A test should then removed from the set of tests believed not to run reliably when it has
    less than min_run executions or has a failure percentage less than acceptable_fr.
    """
    return test_runs < min_run or test_fr <= acceptable_fr


def check_fail_rates(fr_name, acceptable_fr, unacceptable_fr):
    """Raise an error if the acceptable_fr > unacceptable_fr."""
    if acceptable_fr > unacceptable_fr:
        raise ValueError("'{}' acceptable failure rate {} must be <= the unacceptable failure rate"
                         " {}".format(fr_name, acceptable_fr, unacceptable_fr))


def check_days(name, days):
    """Raise an error if days < 1."""
    if days < 1:
        raise ValueError("'{}' days must be greater than 0.".format(name))


def unreliable_tag(task, variant, distro):
    """Returns the unreliable tag."""

    for (component_name, component_value) in (("task", task),
                                              ("variant", variant),
                                              ("distro", distro)):
        if isinstance(component_value, (tf.Wildcard, tf.Missing)):
            if component_name == "task":
                return "unreliable"
            elif component_name == "variant":
                return "unreliable|{}".format(task)
            elif component_name == "distro":
                return "unreliable|{}|{}".format(task, variant)

    return "unreliable|{}|{}|{}".format(task, variant, distro)


def update_lifecycle(lifecycle, report, method_test, add_tags, fail_rate, min_run):
    """Updates the lifecycle object based on the test_method.

    The test_method checks unreliable or reliable fail_rates.
    """
    for summary in report:
        if method_test(summary.fail_rate,
                       fail_rate,
                       summary.num_pass + summary.num_fail,
                       min_run):
            update_tag = unreliable_tag(summary.task, summary.variant, summary.distro)
            if add_tags:
                lifecycle.add_tag("js_test", summary.test, update_tag)
            else:
                lifecycle.remove_tag("js_test", summary.test, update_tag)


def compare_tags(tag_a, tag_b):
    """Compare two tags and return 1, -1 or 0 if 'tag_a' is superior, inferior or
    equal to 'tag_b'.
    """
    return cmp(tag_a.split("|"), tag_b.split("|"))


def validate_config(config):
    """
    Raises a TypeError or ValueError exception if 'config' isn't a valid model.
    """

    for (name, fail_rates) in (("test", config.test_fail_rates),
                               ("task", config.task_fail_rates),
                               ("variant", config.variant_fail_rates),
                               ("distro", config.distro_fail_rates)):
        if not isinstance(fail_rates.acceptable, _NUMBER_TYPES):
            raise TypeError("The acceptable {} failure rate must be a number, but got {}".format(
                name, fail_rates.acceptable))
        elif fail_rates.acceptable < 0 or fail_rates.acceptable > 1:
            raise ValueError(("The acceptable {} failure rate must be between 0 and 1 (inclusive),"
                              " but got {}").format(name, fail_rates.acceptable))
        elif not isinstance(fail_rates.unacceptable, _NUMBER_TYPES):
            raise TypeError("The unacceptable {} failure rate must be a number, but got {}".format(
                name, fail_rates.unacceptable))
        elif fail_rates.unacceptable < 0 or fail_rates.unacceptable > 1:
            raise ValueError(("The unacceptable {} failure rate must be between 0 and 1"
                              " (inclusive), but got {}").format(name, fail_rates.unacceptable))
        elif fail_rates.acceptable > fail_rates.unacceptable:
            raise ValueError(
                ("The acceptable {0} failure rate ({1}) must be no larger than unacceptable {0}"
                 " failure rate ({2})").format(
                     name, fail_rates.acceptable, fail_rates.unacceptable))

    for (name, min_runs) in (("reliable", config.reliable_min_runs),
                             ("unreliable", config.unreliable_min_runs)):
        if not isinstance(min_runs, _NUMBER_TYPES):
            raise TypeError(("The minimum number of runs for considering a test {} must be a"
                             " number, but got {}").format(name, min_runs))
        elif min_runs <= 0:
            raise ValueError(("The minimum number of runs for considering a test {} must be a"
                              " positive integer, but got {}").format(name, min_runs))
        elif isinstance(min_runs, float) and not min_runs.is_integer():
            raise ValueError(("The minimum number of runs for considering a test {} must be an"
                              " integer, but got {}").format(name, min_runs))

    for (name, time_period) in (("reliable", config.reliable_time_period),
                                ("unreliable", config.unreliable_time_period)):
        if not isinstance(time_period, datetime.timedelta):
            raise TypeError(
                "The {} time period must be a datetime.timedelta instance, but got {}".format(
                    name, time_period))
        elif time_period.days <= 0:
            raise ValueError(
                "The {} time period must be a positive number of days, but got {}".format(
                    name, time_period))
        elif time_period - datetime.timedelta(days=time_period.days) > datetime.timedelta():
            raise ValueError(
                "The {} time period must be an integral number of days, but got {}".format(
                    name, time_period))


def update_tags(lifecycle, config, report):
    """
    Updates the tags in 'lifecycle' based on the historical test failures mentioned in 'report'
    according to the model described by 'config'.
    """

    # We initialize 'grouped_entries' to make PyLint not complain about 'grouped_entries' being used
    # before assignment.
    grouped_entries = None
    for (i, (components, rates)) in enumerate(
            ((tf.Report.TEST_TASK_VARIANT_DISTRO, config.distro_fail_rates),
             (tf.Report.TEST_TASK_VARIANT, config.variant_fail_rates),
             (tf.Report.TEST_TASK, config.task_fail_rates),
             (tf.Report.TEST, config.test_fail_rates))):
        if i > 0:
            report = tf.Report(grouped_entries)

        # We reassign the value of 'grouped_entries' to take advantage of how data that is on
        # (test, task, variant, distro) preserves enough information to be grouped on any subset of
        # those components, etc.
        grouped_entries = report.summarize_by(components, time_period=tf.Report.DAILY)

        # Filter out any test executions from prior to 'config.unreliable_time_period'.
        unreliable_start_date = (report.end_date - config.unreliable_time_period
                                 + datetime.timedelta(days=1))
        unreliable_report = tf.Report(entry for entry in grouped_entries
                                      if entry.start_date >= unreliable_start_date)
        update_lifecycle(lifecycle,
                         unreliable_report.summarize_by(components),
                         unreliable_test,
                         True,
                         rates.unacceptable,
                         config.unreliable_min_runs)

        # Filter out any test executions from prior to 'config.reliable_time_period'.
        reliable_start_date = (report.end_date - config.reliable_time_period
                               + datetime.timedelta(days=1))
        reliable_report = tf.Report(entry for entry in grouped_entries
                                    if entry.start_date >= reliable_start_date)
        update_lifecycle(lifecycle,
                         reliable_report.summarize_by(components),
                         reliable_test,
                         False,
                         rates.acceptable,
                         config.reliable_min_runs)


def _split_tag(tag):
    """Split a tag into its components.

    Return a tuple containing task, variant, distro. The values are None if absent from the tag.
    If the tag is invalid, the return value is (None, None, None).
    """
    elements = tag.split("|")
    length = len(elements)
    if elements[0] != "unreliable" or length < 2 or length > 4:
        return None, None, None
    # fillout the array
    elements.extend([None] * (4 - length))
    # return as a tuple
    return tuple(elements[1:])


def _is_tag_still_relevant(evg_conf, tag):
    """Indicate if a tag still corresponds to a valid task/variant/distro combination."""
    task, variant, distro = _split_tag(tag)
    if not task or task not in evg_conf.task_names:
        return False
    if variant:
        variant_conf = evg_conf.get_variant(variant)
        if not variant_conf or task not in variant_conf.task_names:
            return False
        if distro and distro not in variant_conf.distros:
            return False
    return True


def cleanup_tags(lifecycle, evg_conf):
    """Remove the tags that do not correspond to a valid test/task/variant/distro combination."""
    for test_kind in lifecycle.get_test_kinds():
        for test_pattern in lifecycle.get_test_patterns(test_kind):
            if not globstar.glob(test_pattern):
                # The pattern does not match any file in the repository.
                lifecycle.remove_test_pattern(test_kind, test_pattern)
                continue
            for tag in lifecycle.get_tags(test_kind, test_pattern):
                if not _is_tag_still_relevant(evg_conf, tag):
                    lifecycle.remove_tag(test_kind, test_pattern, tag)


def main():
    """
    Utility for updating a resmoke.py tag file based on computing test failure rates from the
    Evergreen API.
    """

    parser = optparse.OptionParser(description=textwrap.dedent(main.__doc__),
                                   usage="Usage: %prog [options] [test1 test2 ...]")

    data_options = optparse.OptionGroup(
        parser,
        title="Data options",
        description=("Options used to configure what historical test failure data to retrieve from"
                     " Evergreen."))
    parser.add_option_group(data_options)

    data_options.add_option(
        "--project", dest="project",
        metavar="<project-name>",
        default=tf.TestHistory.DEFAULT_PROJECT,
        help="The Evergreen project to analyze. Defaults to '%default'.")

    data_options.add_option(
        "--tasks", dest="tasks",
        metavar="<task1,task2,...>",
        help=("The Evergreen tasks to analyze for tagging unreliable tests. If specified in"
              " additional to having test positional arguments, then only tests that run under the"
              " specified Evergreen tasks will be analyzed. If omitted, then the list of tasks"
              " defaults to the non-excluded list of tasks from the specified"
              " --evergreenProjectConfig file."))

    data_options.add_option(
        "--variants", dest="variants",
        metavar="<variant1,variant2,...>",
        default="",
        help="The Evergreen build variants to analyze for tagging unreliable tests.")

    data_options.add_option(
        "--distros", dest="distros",
        metavar="<distro1,distro2,...>",
        default="",
        help="The Evergreen distros to analyze for tagging unreliable tests.")

    data_options.add_option(
        "--evergreenProjectConfig", dest="evergreen_project_config",
        metavar="<project-config-file>",
        default="etc/evergreen.yml",
        help=("The Evergreen project configuration file used to get the list of tasks if --tasks is"
              " omitted. Defaults to '%default'."))

    model_options = optparse.OptionGroup(
        parser,
        title="Model options",
        description=("Options used to configure whether (test,), (test, task),"
                     " (test, task, variant), and (test, task, variant, distro) combinations are"
                     " considered unreliable."))
    parser.add_option_group(model_options)

    model_options.add_option(
        "--reliableTestMinRuns", type="int", dest="reliable_test_min_runs",
        metavar="<reliable-min-runs>",
        default=DEFAULT_CONFIG.reliable_min_runs,
        help=("The minimum number of test executions required for a test's failure rate to"
              " determine whether the test is considered reliable. If a test has fewer than"
              " <reliable-min-runs> executions, then it cannot be considered unreliable."))

    model_options.add_option(
        "--unreliableTestMinRuns", type="int", dest="unreliable_test_min_runs",
        metavar="<unreliable-min-runs>",
        default=DEFAULT_CONFIG.unreliable_min_runs,
        help=("The minimum number of test executions required for a test's failure rate to"
              " determine whether the test is considered unreliable. If a test has fewer than"
              " <unreliable-min-runs> executions, then it cannot be considered unreliable."))

    model_options.add_option(
        "--testFailRates", type="float", nargs=2, dest="test_fail_rates",
        metavar="<test-acceptable-fail-rate> <test-unacceptable-fail-rate>",
        default=DEFAULT_CONFIG.test_fail_rates,
        help=("Controls how readily a test is considered unreliable. Each failure rate must be a"
              " number between 0 and 1 (inclusive) with"
              " <test-unacceptable-fail-rate> >= <test-acceptable-fail-rate>. If a test fails no"
              " more than <test-acceptable-fail-rate> in <reliable-days> time, then it is"
              " considered reliable. Otherwise, if a test fails at least as much as"
              " <test-unacceptable-fail-rate> in <test-unreliable-days> time, then it is considered"
              " unreliable. Defaults to %default."))

    model_options.add_option(
        "--taskFailRates", type="float", nargs=2, dest="task_fail_rates",
        metavar="<task-acceptable-fail-rate> <task-unacceptable-fail-rate>",
        default=DEFAULT_CONFIG.task_fail_rates,
        help=("Controls how readily a (test, task) combination is considered unreliable. Each"
              " failure rate must be a number between 0 and 1 (inclusive) with"
              " <task-unacceptable-fail-rate> >= <task-acceptable-fail-rate>. If a (test, task)"
              " combination fails no more than <task-acceptable-fail-rate> in <reliable-days> time,"
              " then it is considered reliable. Otherwise, if a test fails at least as much as"
              " <task-unacceptable-fail-rate> in <unreliable-days> time, then it is considered"
              " unreliable. Defaults to %default."))

    model_options.add_option(
        "--variantFailRates", type="float", nargs=2, dest="variant_fail_rates",
        metavar="<variant-acceptable-fail-rate> <variant-unacceptable-fail-rate>",
        default=DEFAULT_CONFIG.variant_fail_rates,
        help=("Controls how readily a (test, task, variant) combination is considered unreliable."
              " Each failure rate must be a number between 0 and 1 (inclusive) with"
              " <variant-unacceptable-fail-rate> >= <variant-acceptable-fail-rate>. If a"
              " (test, task, variant) combination fails no more than <variant-acceptable-fail-rate>"
              " in <reliable-days> time, then it is considered reliable. Otherwise, if a test fails"
              " at least as much as <variant-unacceptable-fail-rate> in <unreliable-days> time,"
              " then it is considered unreliable. Defaults to %default."))

    model_options.add_option(
        "--distroFailRates", type="float", nargs=2, dest="distro_fail_rates",
        metavar="<distro-acceptable-fail-rate> <distro-unacceptable-fail-rate>",
        default=DEFAULT_CONFIG.distro_fail_rates,
        help=("Controls how readily a (test, task, variant, distro) combination is considered"
              " unreliable. Each failure rate must be a number between 0 and 1 (inclusive) with"
              " <distro-unacceptable-fail-rate> >= <distro-acceptable-fail-rate>. If a"
              " (test, task, variant, distro) combination fails no more than"
              " <distro-acceptable-fail-rate> in <reliable-days> time, then it is considered"
              " reliable. Otherwise, if a test fails at least as much as"
              " <distro-unacceptable-fail-rate> in <unreliable-days> time, then it is considered"
              " unreliable. Defaults to %default."))

    model_options.add_option(
        "--reliableDays", type="int", dest="reliable_days",
        metavar="<ndays>",
        default=DEFAULT_CONFIG.reliable_time_period.days,
        help=("The time period to analyze when determining if a test has become reliable. Defaults"
              " to %default day(s)."))

    model_options.add_option(
        "--unreliableDays", type="int", dest="unreliable_days",
        metavar="<ndays>",
        default=DEFAULT_CONFIG.unreliable_time_period.days,
        help=("The time period to analyze when determining if a test has become unreliable."
              " Defaults to %default day(s)."))

    parser.add_option("--resmokeTagFile", dest="tag_file",
                      metavar="<tagfile>",
                      default="etc/test_lifecycle.yml",
                      help="The resmoke.py tag file to update. Defaults to '%default'.")

    parser.add_option("--requestBatchSize", type="int", dest="batch_size",
                      metavar="<batch-size>",
                      default=100,
                      help=("The maximum number of tests to query the Evergreen API for in a single"
                            " request. A higher value for this option will reduce the number of"
                            " roundtrips between this client and Evergreen. Defaults to %default."))

    (options, tests) = parser.parse_args()

    if options.distros:
        warnings.warn(
            ("Until https://jira.mongodb.org/browse/EVG-1665 is implemented, distro information"
             " isn't returned by the Evergreen API. This option will therefore be ignored."),
            RuntimeWarning)

    evg_conf = ci_evergreen.EvergreenProjectConfig(options.evergreen_project_config)
    use_test_tasks_membership = False

    tasks = options.tasks.split(",") if options.tasks else []
    if not tasks:
        # If no tasks are specified, then the list of tasks is all.
        tasks = evg_conf.lifecycle_task_names
        use_test_tasks_membership = True

    variants = options.variants.split(",") if options.variants else []

    distros = options.distros.split(",") if options.distros else []

    config = Config(
        test_fail_rates=Rates(*options.test_fail_rates),
        task_fail_rates=Rates(*options.task_fail_rates),
        variant_fail_rates=Rates(*options.variant_fail_rates),
        distro_fail_rates=Rates(*options.distro_fail_rates),
        reliable_min_runs=options.reliable_test_min_runs,
        reliable_time_period=datetime.timedelta(days=options.reliable_days),
        unreliable_min_runs=options.unreliable_test_min_runs,
        unreliable_time_period=datetime.timedelta(days=options.unreliable_days))
    validate_config(config)

    lifecycle = ci_tags.TagsConfig.from_file(options.tag_file, cmp_func=compare_tags)

    test_tasks_membership = get_test_tasks_membership(evg_conf)
    # If no tests are specified then the list of tests is generated from the list of tasks.
    if not tests:
        tests = get_tests_from_tasks(tasks, test_tasks_membership)
        if not options.tasks:
            use_test_tasks_membership = True

    commit_first, commit_last = git_commit_range_since("{}.days".format(options.unreliable_days))
    commit_prior = git_commit_prior(commit_first)

    # For efficiency purposes, group the tests and process in batches of batch_size.
    test_groups = create_batch_groups(create_test_groups(tests), options.batch_size)

    for tests in test_groups:
        # Find all associated tasks for the test_group if tasks or tests were not specified.
        if use_test_tasks_membership:
            tasks_set = set()
            for test in tests:
                tasks_set = tasks_set.union(test_tasks_membership[test])
            tasks = list(tasks_set)
        if not tasks:
            print("Warning - No tasks found for tests {}, skipping this group.".format(tests))
            continue

        test_history = tf.TestHistory(project=options.project,
                                      tests=tests,
                                      tasks=tasks,
                                      variants=variants,
                                      distros=distros)

        history_data = test_history.get_history_by_revision(start_revision=commit_prior,
                                                            end_revision=commit_last)

        report = tf.Report(history_data)
        update_tags(lifecycle, config, report)

    # Remove tags that are no longer relevant
    cleanup_tags(lifecycle, evg_conf)

    # We write the 'lifecycle' tag configuration to the 'options.lifecycle_file' file only if there
    # have been changes to the tags. In particular, we avoid modifying the file when only the header
    # comment for the YAML file would change.
    if lifecycle.is_modified():
        write_yaml_file(options.tag_file, lifecycle)


if __name__ == "__main__":
    main()
