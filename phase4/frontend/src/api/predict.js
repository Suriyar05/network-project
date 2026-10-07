import client from "./api_setup";

const getPredict = async (grid_id, as_of = null) => {
  try {
    const response = await client.post("network/predict-risk", {
      grid_id: grid_id,
      as_of: as_of
    });

    return response.data;
  } catch (error) {
    console.error("Failed to fetch summary:", error);
    console.error("Backend response:", error.response?.data);

    return {
      error: true,
      message: error.message || "An error occurred"
    };
  }
};

export default getPredict;