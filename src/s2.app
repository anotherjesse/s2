{application, s2,
 [{description, "s2"},
  {vsn, "0.01"},
  {modules, [
    s2,
    s2_app,
    s2_sup,
    s2_web,
    s2_deps
  ]},
  {registered, []},
  {mod, {s2_app, []}},
  {env, []},
  {applications, [kernel, stdlib, crypto]}]}.
