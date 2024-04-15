-- Before running drop any existing views
DROP VIEW IF EXISTS q0;

DROP VIEW IF EXISTS q1i;

DROP VIEW IF EXISTS q1ii;

DROP VIEW IF EXISTS q1iii;

DROP VIEW IF EXISTS q1iv;

DROP VIEW IF EXISTS q2i;

DROP VIEW IF EXISTS q2ii;

DROP VIEW IF EXISTS q2iii;

DROP VIEW IF EXISTS q3i;

DROP VIEW IF EXISTS q3ii;

DROP VIEW IF EXISTS q3iii;

DROP VIEW IF EXISTS q4i;

DROP VIEW IF EXISTS q4ii;

DROP VIEW IF EXISTS q4iii;

DROP VIEW IF EXISTS q4iv;

DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era) AS
SELECT
  MAX(era)
FROM
  pitching;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS
SELECT
  namefirst,
  namelast,
  birthyear
FROM
  people
WHERE
  weight > 300;

;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
SELECT
  namefirst,
  namelast,
  birthyear
FROM
  people
WHERE
  namefirst LIKE '% %'
ORDER BY
  namefirst,
  namelast;

;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS
SELECT
  birthyear,
  AVG(height),
  COUNT(*)
FROM
  people
GROUP BY
  birthyear
ORDER BY
  birthyear;

;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS
SELECT
  birthyear,
  AVG(height),
  COUNT(*)
FROM
  people
GROUP BY
  birthyear
HAVING
  AVG(height) > 70
ORDER BY
  birthyear;

;

-- Question 2i
-- poeple: playerid, namefirst, namelast, id...
-- halloffame: playerid, yearid...
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
SELECT
  namefirst,
  namelast,
  playerid,
  yearid
FROM
  people NATURAL
  JOIN halloffame
WHERE
  halloffame.inducted = 'Y'
ORDER BY
  yearid DESC,
  playerid;

;

-- Question 2ii
-- collegeplaying: palyerid, schoolid, yearid
-- schools: schoolid, name_full, city, state, country
CREATE VIEW College(playerid, schoolid) AS
SELECT
  c.playerid,
  c.schoolid
FROM
  collegeplaying c
  INNER JOIN schools s ON c.schoolid = s.schoolid
WHERE
  s.schoolState = 'CA';

-- q2i：namefirst, namelast, playerid, yearid
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
SELECT
  namefirst,
  namelast,
  q.playerid,
  schoolid,
  yearid
FROM
  q2i q
  INNER JOIN College c ON q.playerid = c.playerid
ORDER BY
  yearid DESC,
  schoolid,
  q.playerid;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
SELECT
  q.playerid,
  namefirst,
  namelast,
  schoolid
FROM
  q2i q
  LEFT OUTER JOIN collegeplaying c ON q.playerid = c.playerid
ORDER BY
  q.playerid DESC,
  schoolid;

-- Question 3i
-- batting: playerid, yearid, H, H2B, H3B, HR...
-- H = 1 * 1B + 1 * H2B + 1 * H3B + 1 * HR
-- Note: a player could occur twice (multiple years)
CREATE VIEW slgs(playerid, yearid, AB, slg) AS
SELECT
  playerid,
  yearid,
  AB,
  (H + H2B + 2 * H3B + 3 * HR + 0.0) / (AB + 0.0)
FROM
  batting CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
SELECT
  p.playerid,
  p.namefirst,
  p.namelast,
  s.yearid,
  s.slg
FROM
  people p
  INNER JOIN slgs s ON p.playerid = s.playerid
WHERE
  s.AB > 50
ORDER BY
  s.slg DESC,
  s.yearid,
  p.playerid
LIMIT
  10;

;

-- Question 3ii
CREATE VIEW lslgs(playerid, lslg) AS
SELECT
  playerid,
  (
    SUM(H) + SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) + 0.0
  ) / (SUM(AB) + 0.0)
FROM
  batting
GROUP BY
  playerid
WHERE
  SUM(AB) > 50;

CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
SELECT
  p.playerid,
  p.namefirst,
  p.namelast,
  l.lslg
FROM
  people p
  INNER JOIN lslgs l ON p.playerid = l.playerid
ORDER BY
  l.lslg DESC,
  p.playerid
LIMIT
  10;

;

-- Question 3iii
-- select palyer with lslg higher than Willie Mays
CREATE VIEW q3iii(namefirst, namelast, lslg) AS
SELECT
  p.namefirst,
  p.namelast,
  l.lslg
FROM
  poeple p
  INNER JOIN lslgs l ON p.playerid = l.playerid
WHERE
  l.lslg > (
    SELECT
      lslg
    FROM
      lslgs
    WHERE
      playerid = 'mayswi01'
  );

-- Question 4i
-- salaries: yearid, teamid, lgid, playerid, salary
CREATE VIEW q4i(yearid, min, max, avg) AS
SELECT
  yearid,
  MIN(salary),
  MAX(salary),
  AVG(salary)
FROM
  salaries
GROUP BY
  yearid
ORDER BY
  yearid;

-- Question 4ii
-- create a helper table binids
DROP TABLE IF EXISTS binids;

CREATE TABLE binids(binid);

INSERT INTO
  binids
VALUES
  (0),
  (1),
  (2),
  (3),
  (4),
  (5),
  (6),
  (7),
  (8),
  (9);

-- create a view for bin range
CREATE VIEW binranges AS WITH salaryrange AS (
  SELECT
    MIN(salary) AS minsalary,
    MAX(salary) AS maxsalary
  FROM
    salaries
  where
    yearid = 2016
)
SELECT
  binid,
  minsalary + (maxsalary - minsalary) * binid / 10.0 AS low,
  minsalary + (maxsalary - minsalary) * (binid + 1) / 10.0 AS high
FROM
  binids,
  salaryrange;

-- create a table for bin counts
CREATE TABLE bincounts AS
SELECT
  b.binid,
  CAST(b.low AS INT),
  CAST(b.high AS INT),
  COUNT(s.salaries) AS count
FROM
  binranges b
  LEFT JOIN salaries s ON s.salary >= b.low
  AND (
    s.salary < b.high
    OR b.binid = 9
    AND s.salary <= b.high
  )
WHERE
  s.yearid = 2016
  OR s.yearid IS NULL
GROUP BY
  b.binid;

CREATE VIEW q4ii(binid, low, high, count) AS
SELECT
  bindid,
  low,
  high,
  count
FROM
  bincounts
GROUP BY
  binid;

-- Question 4iii
CREATE VIEW salarystats(year, minsalary, maxsalary, avgsalary) AS
SELECT
  yearid,
  MIN(salary),
  MAX(salary),
  AVG(salary)
FROM
  salary
GROUP BY
  yearid CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
SELECT
  yearid,
  s1.minsalary - s2.minsalary,
  s1.maxsalary - s2.maxsalary,
  s1.avgsalary - s2.avgsalary
FROM
  salarystats s1
  INNER JOIN salarystats s2 ON s1.yearid = s2.yearid - 1
ORDER BY
  s1.yearid;

-- Question 4i
CREATE VIEW maxplayer(playerid, salary, yearid) AS
SELECT
  playerid,
  salary,
  yearid
FROM
  salaries
WHERE
  (
    yearid = 2000
    AND salary = (
      SELECT
        MAX(salary)
      FROM
        salaries s1
      WHERE
        s1.yearid = 2000
    )
  )
  OR (
    yearid = 2001
    AND salary = (
      SELECT
        MAX(salary)
      FROM
        salaries s2
      WHERE
        s2.yearid = 2001
    )
  ) CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
SELECT
  p.playerid,
  p.namefirst,
  p.namelast,
  salary,
  yearid
FROM
  poeple p
  INNER JOIN maxplayer m ON p.playerid = m.playerid;

;

-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
SELECT
  a.teamid,
  MAX(s.salary) - MIN(s.salary)
FROM
  allstartsfull a
  INNER JOIN salaries s on a.playerid = s.palyerid
WHERE
  s.yearid = 2016
GROUP BY
  a.teamid;

;