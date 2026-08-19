export function log(service, message, fields = {}) {
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      service,
      message,
      ...fields,
    }),
  );
}
