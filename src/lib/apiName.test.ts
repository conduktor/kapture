import { describe, expect, it } from "vitest";

import { splitApiName } from "./apiName";

describe("splitApiName", () => {
  it("splits the direction the backend already appended", () => {
    expect(splitApiName("InitProducerIdRequest")).toEqual({
      base: "InitProducerId",
      suffix: "Request",
    });
    expect(splitApiName("FetchResponse")).toEqual({ base: "Fetch", suffix: "Response" });
  });

  it("leaves a name without a direction whole", () => {
    expect(splitApiName("Produce")).toEqual({ base: "Produce", suffix: "" });
    expect(splitApiName("Request")).toEqual({ base: "Request", suffix: "" });
  });
});
