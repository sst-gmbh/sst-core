# Semantic STEP Technology - SST API

## Abstract

SST is based on the W3C **Semantic Web** standards **RDF**, **RDFS** and **OWL**
with **Turtle** (*.ttl files) as the main exchange respectively representation format.
However, unlike other general purpose tools in this field, the goal of SST is not
to support all possible usages of these W3C standards. Instead, the focus of SST
is to effectively support the integration of industrial product data similar to what
is defined in:
* **STEP**, the STandard for the Exchange of Product model data, ISO 10303 series;
* **Oil & Gas**, the standard for "Integration of life-cycle data for process plants
  including oil and gas production facilities”, ISO 15926 series.

### Specific RDF handling
To achieve these goals, the RDF handling in SST is optimized and constrained as follows:
* All *IRI subjects* defined in a *NamedGraph* are composed of a *base IRI* for the *NamedGraph*
  and a *fragment* to indicate the subject node within the NamedGraph.
  This also enforces that the IRI subject is "owned" by the NamedGraph.
  Note that this "ownership" is not enforced by the RDF standard, but is in fact common usage.
* SST is extensively using **UUID**s (Universally unique identifier) for all kind of application data.
  *Random* UUIDs (type 4) usages are to ensure that not two UUID in different places are by accident identical.
  When the same UUID is used in different places the same concept is meant.
  * *Random* UUIDs are used for the *base IRI* of application data in the form of a **UUID-URN**.
    This is to also ensure that the data itself does not declare any kind of owner or location to where the data belongs to.
    Example:
    `urn:uuid:0062e750-abd3-5c7e-adfe-3486fe2fc699`
  * *Random* UUIDs are also used for the fragments of IRI nodes
    Example:
    `urn:uuid:0062e750-abd3-5c7e-adfe-3486fe2fc699#65787e4e-810f-441a-bf46-71a4fec1491e`
  * *Namespace name-based* UUIDs (type 3 and 5) are used to provide a hash for the content and is used for *blank nodes* and *collections*.
* Blank nodes can also be used for predicates. In the W3C recommendation "RDF 1.1 Concepts and Abstract Syntax"
  this extended used is documented under "Generalized RDF Triples, Graphs, and Datasets".
* A **NamedGraph** can directly be based on other *NamedGraphs* by using *owl:import*.
  A **default** NamedGraph together with all imported *NamedGraph*, directly and indirectly,
  form together a **Dataset**. A *Dataset* and its *default NamedGraph* are identified by the same *base IRI*.
* SST stores all RDF data internally in a special binary format that is optimized for processing speed
  and memory consumption. For this the binary data is normalized with strict ordering in all places.
  This also enables very fast calculate of differences between similar files.

### SST Repositories with revision control, using GIT concepts
SST tracks changes of RDF data in an **SST Repository** similar to the capabilities of the revision control system **GIT**.
While *GIT* is using SHA1 for all hash calculation and identification, SST uses SHA256.
While *GIT* can manage any kind of files, an *SST Repository* manages only *binary NamedGraph* files.
The main concepts of an *SST Repository* are:
* **NamedGraphRevision** which is a particular revision of a *binary NamedGraph* that is identified by a SHA256
* **DatasetRevision** that is a revision of a *Dataset*, that identifies
  the default *NamedGraph* and all imported *NamedGraph*s (directly and indirectly) as *NamedGraphRevision*s
* **Commits** that identify who, when and why one or several *DatasetRevision*s have been modified, and what are the parent *commits*.
* An *SST Repository* can either exist
  * on a local computer and be accessed from a local application through the SST API
  * or be hosted on a server computer and be accessed through the **SST Protocol** from another SST client
  * an application can also access a remote *SST Repository* through the SST API
    without the need for a local SST Repository (this capability is not available for GIT).
* Data can arbitrarily be replicated between *SST Repositories* without any loss of consistency.
  This replication might be for all stored data in another repository, or only for some NamedGraphs or some of the history (Commits).

### SST Ontologies
SST comes with a set of pre-defined higher level ontologies. 
These are in hierarchical order:
* the *Fundamental Ontologies* are: 
  * RDF and RDFS for the basic concepts
  * OWL 2 for the logical layer
  * XSD for the datatypes defined in the XML-Schema
  * SSMETA that contains definitions to direct details to the SST-Core on how to operate
  * SKOS, the Simple Knowledge Organization System Reference
  * SH to represent SHACL data model constraints; no yet implemented
* the *Application Independent Ontologies* are on top of the fundamental ontologies:
  * LCI "Live Cycle Integration" that is derived from the ontology defined in ISO/TS 15926-12 to support the concepts defined in the data model of ISO 10303-2
  * Quantities & Units as defined in ISO 80000
  * Country codes as defined in ISO 3166
  * Currency codes as defined in ISO 4217
* the "STEP Specific Ontologies" are on top of the Application Independent Ontologies:
  * ssrep for all STEP definitions on "representation", based on ISO 10303-43. This includes geometry, topology, presentation/styling, kinematics and others
  * sso for the STEP PDM (Product Data Management) concepts
  * eed for Schematic Diagrams
* Reference data
  * IEC Common Data Dictionary as defined in IEC 61360 (draft)
  * ... more reference data sources to be added over time


### SST Compiler, late & early binding

Like other RDF tools the SST API provides support of arbitrary ontologies 
by using the URI of the referenced resources that are provided as STRING to the API methods. 
This is called **late binding** because only when running an application the API can check if the provided string is valid.

SST provides special support for the above listed SST Ontologies by using the SST Compiler. 
Primarily the SST Compiler is converting an SST Ontology (provides as ttl file) into corresponding GO code, 
so that the concepts defined in an ontology are directly accessible as GO elements. 
This is called **early binding**; meaning that already at the GO compile time of an SST application 
basic consistency with the referenced ontology elements can be checked.

Example: 
In the "RDF Schema vocabulary" the concept __<http://www.w3.org/2000/01/rdf-schema#label>__ is defined. 
In a Turtle (ttl) file this is typically abbreviated as __rdfs:label__.
In SST early binding it is sufficient to use the GO code __ssrdfs.Label__ for the same concept. 

SST does not rely on any extensice inferencing that is otherwise needed when using OWL. 
This is achieved by another output of the SST Compiler that is generating efficient data structures to
dynamically provide answers about:
* hierachy of rdfs:subClassOf in combination with rdf:type
* hierachy of rdsf:subPropertyOf in combination with with owl:inverseOf


### API
The **SST API** is the default API to access and manipulate data in an *SST Repository*.
It is written in the GO programming language (Golang) and also allows importing and exporting from/to other formats
such as *RDF-Turtle* and STEP.



## Terms and Definitions

The **Resource Description Framework
([RDF 1.1 Concepts and Abstract Syntax](https://www.w3.org/TR/rdf11-concepts/))
provides essential definitions that are essential for the understanding of SST:
* **RDF Graph**:a sets of subject-predicate-object triples
* **Triple**: consisting of a **subject**, **predicate** and **object** element
  that are either IRIs, blank nodes, or datatyped literals
* **IRI**: Internationalized Resource Identifier
* IRIs with **Fragment Identifiers**, or in short "fragment"
* **literal** and literal value
* **blank node**
* RDF datasets, consisting of one default graph and zero or more named graph

SST used in addition the following terms:
* **IBNode** that is either an IRI node or a blank node
* **Dataset** that is an RDF dataset identified by an IRI and consisting
  of a default graph that is typically importing other named graph

## Overview



SST organises RDF/OWL data in memory as follows:

* an **RDF graph** consists of RDF triples

* **RDF triples** consists of the three parts **subject**, **predicate** and **object**
  where subject and predicate are always of type IBNodes
  while the object might be an **IBNode**, **Literal** or **Collection**.

* an IBNode is either an **IRI node** or a **blank node**.

* an IBNode

* the SST **triplex structure** allows traversing of the RDF triples that makes up an RDF graph
  in any direction,
  either starting from the subject, predicate or object if it is an IBNode.

* an IBNode

* an IBNode together with all triples with this IBNode as subject
  are owned by the NamedGraph of which the IBNode is a fragment of.
  All RDF triples together with the used Literals, Collections and blank nodes are owned
  by the NamedGraph that also owns the subject

* IBNodes, Literals and Collections are stored within a **NamedGraph**
  that is "owning" these objects. The owning NamedGraph

* Every **NamedGraph** is the default RDF Graph for a corresponding *Dataset*
  in a one to one relationship.
  A *Dataset* and its default *NamedGraph* are identified by a IRI.

* A *NamedGraph* can import other *NamedGraph* with **owl:import**.
  A default *NamedGraph* together with all imported (directly and indirectly)
  other *NamedGraphs* form together a **Dataset**.

* An **SST Repository** stores revisions of *Datasets* and *NamedGraphs*
  together with corresponding **Commit**s by which these
  *Datasets* and *NamedGraphs* have been created.

* A revision of a *Dataset* together with
  the revision of its corresponding default NamedGraph
  and all imported other NamedGraphs (directly and indirectly)
  are made available in memory as a **Stage**.
  The data in a Stage can be modified and then stored by a commit 
  as new revisions of *Datasets* and *NamedGraphs* in the SST Repository that is linked to the Stage.

SST organises RDF/OWL data in an SST Repository as follows:

* a binary NG-SST file stores a particular revision of a NamedGraph
  together with all RDF triples
  Binary NG-SST files are self-contained and include all references to other NamedGraphs.
  Binary NG-SST files are identified by their SHA256 key.

* A commit contains:
  * a timestamp when the data was created or changed
  * the identity of the committer and author of the data
  * a comment describing the changes or reasons of the commit
  * the affect *Dataset* revisions

* Change history is recorded by commits that are chained with each other by parent Commits.
  A Commit might have 0, 1 or several parent Commits.
  If there is no parent Commit, then newly created *Datasets* are identified.
  If there is a single parent Commit, then simple modifications of the affected *Datasets* are identified.
  If there are several parent Commits, two different cases might happen:

  * the Commits are on different *Datasets*. .... TBD
  * the Commit represents a **Merge** actions of two or more revisions.
    ISSUE? what if adding a new import

* Creating new *Datasets*, *NamedGraphs*, *IBNodes*, ... can be done
  in a **Stage** before the first Commit

