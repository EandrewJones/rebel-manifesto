Project Scope
=============

**Hypothesis:** 

Nationalist groups have transitioned over time to root their claims more firmly in the human rights realm, as opposed to claims about a 
national homeland.  

**Thoughts/Questions:**

The first step will be to identify the type of data we have available.

We are presumably interested in comparing nationalist groups versus *all other* possible groups. Excuse my ignorance, is there a standard categorization of different groups within the literature? **Is there some categorization list that can easily be obtained so that we can classify what groups we have and their distribution?**

The data is a mixture of primary and secondary sources. *We should avoid secondary sources where possible* for numerous reasons. 

First, they are very unstructured and inconsistent depending on the source which makes ingestion and analysis difficult. You have to develop a bespoke approach for extracting meaningful information from each of them. Second, they are not temporally-anchored. An article from 1990 might draw on selectively chosen statements over the course of many years. These statements may be paraphrased or quoted of context to advance a researcher or journalistic narrative. Both are problematic. In the exceptional case they include direct statements, we would need to be able to identify the date of the statement. This will again vary by source and extracting them in an algorithmic way will be cumbersome. Finally, secondary analyses are already filtered/biased by researcher degrees of freedom.

If we are to stick with primary sources, this will likely create an imbalance. Smaller, more obscure, and/or short-lived groups will likely have less in the way of primary source statements. Conversely, established, well-organized and funded groups  will have more abundant data. Take Hamas for example. Their website has statements from 2015-present. Older statements will likely be more readily available too. 

All of this points to availability-induced selection bias. **Are you okay with selection bias in the data?** In short, we almost certainly won't be able to build a comprehensive dataset, but for the sake of testing your hypothesis I think we should aim to:

1. Match the overall distribution of nationalist vs. non-nationalist groups in the population
2. Have as representative as possible sample across regions
3. Bias towards groups for which we can find statements distributed over time

The third point gets to a final question I have about the temporal dimension of you hypothesis. **Do you mean time in a relative or absolute sense?** For instance, do you think the evolution towards human rights-rooted legitmacy is a natural evolution that all nationalist groups exhibit, regardless of when they are founded. Or is there something about the march of international relations over time, i.e. world events and history that has shaped claims? Physics quibbles aside, this distinction matters theoretically but also methodologically because it will determine our data collection and modeling strategy

Once we have a better overview of the meta-data distribution of our data we can figure out the best approach forward. 

My Role
========

- accessing the raw data collected by other scholars (I've attached their just released paper here and they are sending and excel file with links to reports)
- possibly gathering more raw data from non-rebel actors (there may be existing work on this for political parties)
- determining the best way to evaluate the hypothesis with this data. 


Technical Notes
===============

Python Packages for Parsing PDF
-------------------------------

- PyPDF2, textract, PDFMiner

Analysis Approaches
-------------------

- Dynamic Embedded Topic Models
- Handles variation in topics over time, but cannot control for source or source-level covariates
- Maybe STM

To Do
=====

1. Make a meta-data set of all the groups we currently have data on. Include: country, secondary versus primary, group categorization, and time range of available documents