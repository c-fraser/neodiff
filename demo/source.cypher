// Create constraints
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE;

// Create people
CREATE (keanu:Person {name: 'Keanu Reeves', born: 1964});
CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967});
CREATE (laurence:Person {name: 'Laurence Fishburne', born: 1961});
CREATE (hugo:Person {name: 'Hugo Weaving', born: 1960});
CREATE (lilly:Person {name: 'Lilly Wachowski', born: 1967});
CREATE (lana:Person {name: 'Lana Wachowski', born: 1965});
CREATE (tom:Person {name: 'Tom Hanks', born: 1956});
CREATE (meg:Person {name: 'Meg Ryan', born: 1961});
CREATE (nora:Person {name: 'Nora Ephron', born: 1941});

// Create movies
CREATE (matrix:Movie {title: 'The Matrix', released: 1999, tagline: 'Welcome to the Real World'});
CREATE (reloaded:Movie {title: 'The Matrix Reloaded', released: 2003, tagline: 'Free your mind'});
CREATE (mail:Movie {title: 'You\'ve Got Mail', released: 1998, tagline: 'At last... aass you can\'t click off'});
CREATE (sleepless:Movie {title: 'Sleepless in Seattle', released: 1993, tagline: 'What if someone you never met...'});

// Create relationships
MATCH (keanu:Person {name: 'Keanu Reeves'}), (matrix:Movie {title: 'The Matrix'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (matrix:Movie {title: 'The Matrix'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(matrix);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (matrix:Movie {title: 'The Matrix'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(matrix);

MATCH (hugo:Person {name: 'Hugo Weaving'}), (matrix:Movie {title: 'The Matrix'})
CREATE (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(matrix);

MATCH (lilly:Person {name: 'Lilly Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lilly)-[:DIRECTED]->(matrix);

MATCH (lana:Person {name: 'Lana Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lana)-[:DIRECTED]->(matrix);

MATCH (keanu:Person {name: 'Keanu Reeves'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(reloaded);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(reloaded);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(reloaded);

MATCH (lilly:Person {name: 'Lilly Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lilly)-[:DIRECTED]->(reloaded);

MATCH (lana:Person {name: 'Lana Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lana)-[:DIRECTED]->(reloaded);

MATCH (tom:Person {name: 'Tom Hanks'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (tom)-[:ACTED_IN {roles: ['Joe Fox']}]->(mail);

MATCH (meg:Person {name: 'Meg Ryan'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (meg)-[:ACTED_IN {roles: ['Kathleen Kelly']}]->(mail);

MATCH (nora:Person {name: 'Nora Ephron'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (nora)-[:DIRECTED]->(mail);

MATCH (tom:Person {name: 'Tom Hanks'}), (sleepless:Movie {title: 'Sleepless in Seattle'})
CREATE (tom)-[:ACTED_IN {roles: ['Sam Baldwin']}]->(sleepless);

MATCH (meg:Person {name: 'Meg Ryan'}), (sleepless:Movie {title: 'Sleepless in Seattle'})
CREATE (meg)-[:ACTED_IN {roles: ['Annie Reed']}]->(sleepless);

MATCH (nora:Person {name: 'Nora Ephron'}), (sleepless:Movie {title: 'Sleepless in Seattle'})
CREATE (nora)-[:DIRECTED]->(sleepless);
