{application, s2,
[
  {description, "S2: Stupid S3"},
  {vsn, '0.0.1'},
  {modules, [req, bucket, storage, meta]},
  {registered, [req]},
  {env, []},
  {applications, [kernel, stdlib]}
]}.
