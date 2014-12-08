![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-recom/master/images/dr_kruscheundpartner_640.png)

## Reactive Recommender System

This project comprises a recommender system based on **Predictiveworks.** and is implemented on top of Apache Spark, 
Elasticsearch and Typesafe's Akka and Spray.

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics 
requirements. [More...](http://predictiveworks.eu)

Recommenders have a strong focus on user item interactions, and can be derived by creating explicit user and item profiles to 
characterize their nature. These profiles allow to associate users with matching items and, from that data, specify user item 
interactions (*content filtering*).

An alternative approach that avoid the extensive gathering of external profile data is based on the extraction of inherent 
and latent data from the underlying dataset to specify users, items and their interactions.

The Predictiveworks recommender uses **latent data** and supports two different approaches to personalized recommendation:

### Latent Relations

This approach uncovers latent relations between items in a large scale dataset und combines these relations with the 
last user transaction data. 

It is based on Association Rule Mining and uses the **Top-K NR** algorithm from [Philippe-Fournier Viger](http://www.philippe-fournier-viger.com/).

In 2012, Philippe-Fournier Viger redefined the problem of association rule mining as **Top-K Association Rule Mining**. The 
proposed algorithm only depends on the parameters *k*, the number of rules to be generated, and *minimum confidence*, and circumvents 
the well-known *minimum support* problem.

### Latent Factors

This approach learns latent factors for each user and item from the underlying dataset and uses these factors to specify user 
item interactions (*matrix factorization*). In addition to learning latent factors for users and items, the Predictiveworks 
recommender is also capable uncovers factors for additional features that describe the context of a user item engagement.

The latter approach is based on **factorization models**. Factorization models are a generalization of matrix factorization and 
are capable of modeling complex relationships in the data to provide **context-aware** personalized recommendations.