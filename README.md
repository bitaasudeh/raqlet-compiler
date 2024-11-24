# PG-Schema Parser

## Description

**PGSchemaParser** is a Scala-based parser for graph schemas, designed to parse and extract information from graph-based schema definitions. It supports custom schema types such as `NodeType` and `EdgeType`.

## Features

* Parses and extracts graph schema definitions into `NodeType` and `EdgeType` objects.
* Produces the output of the parsed schema in a human-readable format.

## Installation and usage

**sbt** (Simple Build Tool) is utilized for compiling, running, testing, and packaging, a build tool specifically designed for Scala and Java projects.

1. Extract `raqlet-compiler.zip` into your **home directory**. 
2. cd to the `compiler` directory which contains `build.sbt`
3. Open the **terminal** and  **sbt** .

```shell
PS C:\Users\inwt\raqlet-compiler\raqlet-compiler\compiler> sbt
[info] welcome to sbt 1.7.3 (Oracle Corporation Java 17.0.12)
[info] loading settings for project compiler-build from plugins.sbt ...
[info] loading project definition from C:\Users\inwt\raqlet-compiler\raqlet-compiler\compiler\project
[info] loading settings for project compiler from build.sbt ...
[info] set current project to raqlet (in build file:/C:/Users/inwt/raqlet-compiler/raqlet-compiler/compiler/)
[info] sbt server started at local:sbt-server-7d424cade77b4577d139
[info] started sbt server
sbt:raqlet>
```

4. To compile the project, type the `compile` command.

```shell
sbt:raqlet> compile
```

5. To run the project, use the `run` command. (For Unix-like systems, use `/` instead of `\\`.)

```shell
sbt:raqlet> run \\raqlet-compiler\\compiler\\src\\test\\pgschema pgschema
```

6. To run tests, type the `test` command.

```shell
sbt:raqlet> test
```

## Project Structure

```shell
C:.
│   build.sbt
│   README.md
│
└───src
    ├───main
    │   └───scala
    │       └───uk
    │           └───ac
    │               └───ed
    │                   └───dal
    │                       └───raqlet
    │                           │   Main.scala
    │                           │
    │                           ├───ir
    │                           │       CyphExpr.scala
    │                           │       CyphSchema.scala
    │                           │       DatalogExpr.scala
    │                           │       DatalogOptExpr.scala
    │                           │       SqlExpr.scala
    │                           │       SqlogExpr.scala
    │                           │
    │                           ├───parser
    │                           │   │   CypherParser.scala
    │                           │   │   DatalogParser.scala
    │                           │   │
    │                           │   └───pgschema
    │                           │           FeatureInheritance.scala
    │                           │           LabelCorrectionInheritance.scala
    │                           │           PGSParser.scala
    │                           │
    │                           ├───transformer
    │                           │       CyphAstLower.scala
    │                           │       CypherToDatalog.scala
    │                           │       CyphRewrite.scala
    │                           │       DatalogOptimiser.scala
    │                           │       DatalogRewrite.scala
    │                           │       SqlogLower.scala
    │                           │       SqlogRewrite.scala
    │                           │
    │                           └───unparser
    │                                   CyphUnparser.scala
    │                                   DatalogUnparser.scala
    │                                   SqlUnparser.scala
    │
    └───test
        ├───cypher
        │       complex1.cypher
        │       complex10.cypher
        │       complex11.cypher
        │       complex12.cypher
        │       complex13.cypher
        │       complex2.cypher
        │       complex3.cypher
        │       complex4.cypher
        │       complex5.cypher
        │       complex6.cypher
        │       complex7.cypher
        │       complex8.cypher
        │       complex9.cypher
        │       short1.cypher
        │       short2.cypher
        │       short3.cypher
        │       short4.cypher
        │       short5.cypher
        │       short6.cypher
        │       short7.cypher
        │
        ├───datalog
        │   │   complex1.dl
        │   │   complex10.dl
        │   │   complex11.dl
        │   │   complex12.dl
        │   │   complex13.dl
        │   │   complex2.dl
        │   │   complex3.dl
        │   │   complex4.dl
        │   │   complex5.dl
        │   │   complex6.dl
        │   │   complex7.dl
        │   │   complex8.dl
        │   │   complex9.dl
        │   │   short1.dl
        │   │   short2.dl
        │   │   short3.dl
        │   │   short4.dl
        │   │   short5.dl
        │   │   short6.dl
        │   │   short7.dl
        │   │
        │   └───output
        │           complex1.dl
        │           complex10.dl
        │           complex11.dl
        │           complex12.dl
        │           complex13.dl
        │           complex2.dl
        │           complex3.dl
        │           complex4.dl
        │           complex5.dl
        │           complex6.dl
        │           complex7.dl
        │           complex8.dl
        │           complex9.dl
        │           short1.dl
        │           short2.dl
        │           short3.dl
        │           short4.dl
        │           short5.dl
        │           short6.dl
        │           short7.dl
        │
        ├───pgschema
        │       no_errors_modified.txt
        │       test01.txt
        │       test02.txt
        │       test03.txt
        │
        └───scala
            └───uk
                └───ac
                    └───ed
                        └───dal
                            └───raqlet
                                │   GeneratedLDBC.scala
                                │   LDBC.scala
                                │
                                ├───parser
                                │       CypherParserTest.scala
                                │       DatalogParserTest.scala
                                │       PGParserTest.scala
                                │
                                └───transformer
                                        CypherToDatalogTest.scala
                                        CypherToDatalogTestLDBCGenerated.scala
                                        DatalogOptimiserTest.scala
                                        DatalogOptimiserTestLDBCGenerated.scala
                                        DatalogToSQLTest.scala
                                        DatalogToSQLTestLDBCGenerated.scala
```

## Examples

Here is an input example and its expected output to illustrate how the parser operates:

### Input:

```shell
CREATE GRAPH TYPE FraudGraphType LOOSE {
  (PersonType: Person {name STRING}),
  (CustomerType: PersonType & Customer {c_id INT32}),
  (CreditCardType: CreditCard {cc_num STRING}),
  (TransactionType: Transaction {cc_num STRING}),
  (AccountType: Account {acct_id INT32}),
  (:CustomerType)-[OwnsAccountType: owns]->(:AccountType),
  (:CustomerType)-[UsesCreditCardType: uses]->(:CreditCardType),
  (:TransactionType)-[ChargesCreditCardType: charge {amount DOUBLE}]->(:CreditCardType),
  (:TransactionType)-[ActivityType: deposit|withdraw {time DATETIME}]->(:AccountType)
}
```

###  output:

```shell
FraudGraphType(Seq(
	NodeType("Person", LabelInput("Person"), Seq("name" -> STRING), "Person.facts"), 
	NodeType("Customer", LabelInput("Customer"), Seq("c_id" -> INT32), "Customer.facts"), 
	NodeType("CreditCard", LabelInput("CreditCard"), Seq("cc_num" -> STRING), "CreditCard.facts"), 
	NodeType("Transaction", LabelInput("Transaction"), Seq("cc_num" -> STRING), "Transaction.facts"), 
	NodeType("Account", LabelInput("Account"), Seq("acct_id" -> INT32), "Account.facts"), 
	EdgeType(LabelInput("Customer"), LabelInput("Account"), LabelInput("Owns"), "OwnsAccount.facts"), 
	EdgeType(LabelInput("Customer"), LabelInput("CreditCard"), LabelInput("Uses"), "UsesCreditCard.facts"), 
	EdgeType(LabelInput("Transaction"), LabelInput("CreditCard"), LabelInput("Charge"), Seq("amount" -> DOUBLE), "ChargesCreditCard.facts"), 
	EdgeType(LabelInput("Transaction"), LabelInput("Account"), LabelOr(List(LabelInput("Deposit"), LabelInput("Withdraw"))), Seq("time" -> DATETIME), "Activity.facts"))
)
```

