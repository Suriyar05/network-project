import client from "./api_setup";

const getHotspots = async (
  limit = 10,
  severity = "",
  as_of = null
) => {
  try {
    const params = {};

    if (limit) params.limit = limit;
    if (severity) params.severity = severity;
    if (as_of) params.as_of = as_of;

    const response = await client.get("/network/hotspots", { params });

    return response.data;
  } catch (error) {
    console.error("Failed to fetch hotspots:", error);

    return {
      error: true,
      message:
        error.response?.data?.detail ||
        error.message ||
        "An error occurred",
    };
  }
};

const getAlerts = async (
  limit = 10,
  severity = "",
  as_of = null
) => {
  try {
    const params = {};

    if (limit) params.limit = limit;
    if (severity) params.severity = severity;
    if (as_of) params.as_of = as_of;

    const response = await client.get("/network/alerts", { params });

    return response.data;
  } catch (error) {
    console.error("Failed to fetch alerts:", error);

    return {
      error: true,
      message:
        error.response?.data?.detail ||
        error.message ||
        "An error occurred",
    };
  }
};

export { getHotspots, getAlerts };