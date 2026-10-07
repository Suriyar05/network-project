import client from "./api_setup";

const getNetworkSummary = async (as_of = null) => {
  try {
    const response = await client.get("network/summary", {
      params: as_of ? { as_of } : {}
    });

    return response.data;
  } catch (error) {
    console.error("Failed to fetch summary:", error);
    return {
      error: true,
      message: error.message || "An error occurred"
    };
  }
};

export default getNetworkSummary;