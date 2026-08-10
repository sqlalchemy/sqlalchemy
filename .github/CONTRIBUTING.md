# Contributing to SQLAlchemy

For general developer guidelines, please see out current Developer Guide at
[Develop](https://www.sqlalchemy.org/develop.html).

## Pull requests require an approved issue ##

**We accept pull requests only for issues that a maintainer has marked with
the `open for pull requests` label.**  A pull request that doesn't reference
such an issue is closed automatically, by a bot, as soon as it's opened.

The reason is that the vast majority of unsolicited pull requests are
solutions to a problem we haven't agreed on yet, and reviewing them costs far
more than writing them does.  Settling the approach on the issue first means
nobody writes code that was never going to be merged.

The process is:

1. **Open an issue** describing the problem or the feature, with a complete,
   runnable example.  For a bug, that means a script we can run that shows the
   wrong behavior.
2. **Wait for a maintainer** to look at it.  If we agree the change is wanted
   and that an outside contribution is the right way to get it, we add the
   `open for pull requests` label.
3. **Then open the pull request**, referencing the issue.

Some labels you'll see on issues, and what they mean for contributors:

| label | meaning |
| --- | --- |
| `open for pull requests` | we'd welcome a pull request for this |
| `code review in progress` | someone is already working on it, in a pull request or directly in gerrit |
| `NO pull requests please` | we're handling this one ourselves; please don't |

An issue with none of these labels hasn't been decided on yet.  Asking on the
issue is fine; opening a pull request to make the case is not, and it will be
closed.

This applies to documentation and typo fixes too.  We know that feels heavy
for a one-line change, but the alternative is a rule with exceptions that have
to be argued about, and we'd rather have one rule.  A typo is a two-line issue
and we'll label it quickly.

## Note on use of AI, agents and bots ##

Some of us here use large language models (LLM) to help us with our work, and
some of us are even employer mandated to do so.   Getting help whereever you
need is fine.

However we must ask that **AI/LLM generated content is not spammed onto SQLAlchemy
discussions, issues, or PRs**, whether this is cut and pasted, fully automated,
or even just lightly edited.   **Please use your own words and don't come
off like you're a bot**, because that only makes you seem like you're trying
to gamify our organization for unearned gain.

In particular, **users who post content that appears to be trolling for karma /
upvotes / vanity commits / positive responses, whether or not this content is
machine generated, will be banned**.  We are not a casino and we're not here
to be part of gamification of any kind.


