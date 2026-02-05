# Dashboard DB Analysis & API Feature Recommendations

## 1. Database structure summary

### Core entities

| Entity | Table(s) | Key columns | Approx. rows |
|--------|----------|-------------|---------------|
| **Ecosystems** | `ecosystems` | id, name, launch_date, derived_launch_date, is_crypto, is_category, is_chain, is_multichain | ~13k |
| **Developers** | `canonical_developers`, `user_info` | id, primary_github_user_id; + login, name, company, location (user_info) | ~3.1M devs, ~3.1M user_info |
| **Repos** | `repos` | id, name, link, organization_id, num_stars, num_forks | ~2.4M |
| **Organizations** | `organizations` | id, name, link | — |

### Relationships (ecosystem ↔ repos ↔ developers)

| Link | Table(s) | Purpose |
|------|----------|---------|
| Ecosystem → Repos | `ecosystems_repos`, `ecosystems_repos_recursive` | Which repos belong to an ecosystem (direct + recursive) |
| Ecosystem → Child ecosystems | `ecosystems_child_ecosystems`, `ecosystems_child_ecosystems_recursive` | Parent/child hierarchy (e.g. Bitcoin → Lightning) |
| Ecosystem → Orgs | `ecosystems_organizations` | Orgs tied to an ecosystem (is_first_party) |
| Repo → Developers (activity) | `developer_activities`, `repo_developer_activities`, `repo_developer_28d_activities` | Commits per dev per repo/day |
| **Ecosystem → Developers (activity)** | `eco_developer_activities`, `eco_developer_28d_activities` | Commits per dev per ecosystem/day |
| **Ecosystem → Developers (rank)** | `eco_developer_contribution_ranks` | contribution_rank: full_time / part_time / one_time; points |
| Ecosystem → Developers (tenure) | `eco_developer_tenures` | tenure_days, category per dev per ecosystem |
| **Ecosystem aggregates (daily)** | `eco_mads` | all_devs, exclusive_devs, num_commits, full_time_devs, etc. per ecosystem/day |
| Commits | `commits` | Per-repo, per canonical_developer_id, additions/deletions, dates |

### Developer identity & profile

- `canonical_developers`: id, primary_developer_email_identity_id, primary_github_user_id  
- `user_info`: canonical_developer_id, login, name, company, location, url, email, primary_github_user_id  
- `canonical_developer_locations`: canonical_developer_id, country, admin_level_1, lat/lng, etc.

---

## 2. How this maps to a “explore ecosystems and developers” dashboard

- **Ecosystem list & drill-down**: List/filter ecosystems → select one → see repos, child ecosystems, and developer activity.
- **Developer list & drill-down**: List/filter developers in an ecosystem (with rank: full_time / part_time / one_time) → see profile (user_info), repos, commits, tenure.
- **Time series**: Use `eco_mads` and `eco_developer_activities` / `eco_developer_contribution_ranks` for trends over time (activity, dev counts, rank distribution).

---

## 3. Recommended opendev_api features for the dashboard

### A. Ecosystems

| Feature | Purpose | Suggested query shape |
|--------|---------|------------------------|
| **List ecosystems** | Browse/search ecosystems for the dashboard | Paginated; filter by name, is_crypto, is_chain, etc.; optionally include repo_count or latest eco_mads row. |
| **Get ecosystem by id** | Header/detail for “Ecosystem X” view | Single row from `ecosystems`; optionally join latest `eco_mads` (all_devs, num_commits). |
| **Ecosystem hierarchy** | Show parent/children in UI | From `ecosystems_child_ecosystems` or `ecosystems_child_ecosystems_recursive` (parent_id, child_id); resolve to ecosystem names. |
| **Repos in ecosystem** | “Repos in this ecosystem” table/list | From `ecosystems_repos` or `ecosystems_repos_recursive` + `repos`; sort by num_stars/name; paginate. |
| **Ecosystem time series (MADs)** | Charts: dev count, commits over time | From `eco_mads`: filter by ecosystem_id, day range; return day, all_devs, exclusive_devs, num_commits, full_time_devs, etc. |

### B. Developers in ecosystems

| Feature | Purpose | Suggested query shape |
|--------|---------|------------------------|
| **Developers in ecosystem** | “Top/active developers in Ecosystem X” | Join `eco_developer_contribution_ranks` + `canonical_developers` (+ optional `user_info`). Filter by ecosystem_id, optionally day range; sort by points/rank; paginate. |
| **Developer profile** | Developer detail page | By canonical_developer_id: `user_info` + optional `canonical_developer_locations`; optionally recent activity. |
| **Developer activity in ecosystem** | Commits / activity over time for one dev in one ecosystem | From `eco_developer_activities` or `eco_developer_28d_activities`: filter by ecosystem_id, canonical_developer_id, day range; aggregate or daily. |
| **Developer tenure in ecosystem** | “How long has this dev been in this ecosystem?” | From `eco_developer_tenures`: filter by ecosystem_id, canonical_developer_id; return tenure_days, category, day. |

### C. Cross-cutting / search

| Feature | Purpose | Suggested query shape |
|--------|---------|------------------------|
| **Search ecosystems** | Type-ahead or search box | Filter `ecosystems` by name (ILIKE / contains); limit 20–50. |
| **Search developers** | Find dev by login/name in an ecosystem | Join `user_info` + `eco_developer_contribution_ranks` (or activities) filtered by ecosystem_id; filter by login/name; paginate. |
| **Top repos in ecosystem** | “Starred repos” or “most active” | `ecosystems_repos` + `repos`; sort by num_stars or by commit count from `developer_activities`/`commits`; limit. |

### D. Data already supported by your API

- **user_info** is populated by `create_user_info_table`; use it for developer profile (login, name, company, location, url, email) in “developer profile” and “developers in ecosystem” (when joined to canonical_developer_id).

---

## 4. Suggested implementation order for the API

1. **Ecosystems**
   - List ecosystems (with optional filters and repo count).
   - Get ecosystem by id (with optional latest MADs).
   - Repos in ecosystem (paginated).
2. **Developers in an ecosystem**
   - List developers in ecosystem (from contribution_ranks + user_info), with rank and points.
   - Get developer profile (user_info + optional location).
3. **Time series**
   - Eco MADs over time for an ecosystem (for charts).
   - Optional: developer activity over time in an ecosystem.
4. **Hierarchy & search**
   - Ecosystem parent/children.
   - Search ecosystems by name.
   - Search developers by login/name within an ecosystem.

---

## 5. Query performance notes

- **eco_developer_contribution_ranks** and **eco_developer_activities** are very large (~3.7B and ~703M rows). Always filter by `ecosystem_id` and preferably by `day` (or a recent window) and use `LIMIT`/pagination.
- **eco_mads** is smaller (~10M rows) and keyed by ecosystem + day; good for dashboard time series.
- Add indexes (if not present) on:
  - `(ecosystem_id, day)` for eco_mads, eco_developer_activities, eco_developer_contribution_ranks
  - `(ecosystem_id, canonical_developer_id)` for developer-in-ecosystem lookups
  - `(canonical_developer_id)` for user_info / profile lookups

This structure and feature set should be enough for a dashboard where users explore ecosystems and the developers active in those ecosystems, with time series and drill-downs.
