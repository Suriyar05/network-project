import client from "./api_setup";

const getNetworkGrid = async (
  grid_id,
  date = null,
  hour = null,
  as_of = null
) => {
  try {
    let params = {};

    if (date !== null) {
      params.date = date;
    }

    if (hour !== null) {
      params.hour = hour;
    }

    if (as_of !== null) {
      params.as_of = as_of;
    }

    const response = await client.get(
      `/network/grid/${grid_id}`,
      {
        params: params
      }
    );

    return response.data;

  } catch (error) {
    console.error(
      `Failed to fetch grid ${grid_id}:`,
      error
    );

    return {
      error: true,
      message:
        error.response?.data?.detail ||
        error.message ||
        "An error occurred"
    };
  }
};

export default getNetworkGrid;