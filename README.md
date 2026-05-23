# b2-cosi-driver

A [COSI](https://container-object-storage-interface.sigs.k8s.io/) driver for [Backblaze B2](https://www.backblaze.com/cloud-storage) object storage.

## Prerequisites

Install the COSI CRDs and controller in your cluster:

```sh
kubectl apply -k 'github.com/kubernetes-sigs/container-object-storage-interface?ref=v0.2.2'
```

## Deployment

Create the namespace and a secret with your B2 application key:

```sh
kubectl create namespace b2-cosi-system

kubectl -n b2-cosi-system create secret generic b2-credentials \
  --from-literal=applicationKeyId=<your-key-id> \
  --from-literal=applicationKey=<your-key>
```

Deploy the driver and RBAC:

```sh
kubectl apply -f https://raw.githubusercontent.com/bo0tzz/b2-cosi-driver/v0.0.4/config/rbac.yaml       # x-release-please-version
kubectl apply -f https://raw.githubusercontent.com/bo0tzz/b2-cosi-driver/v0.0.4/config/deployment.yaml # x-release-please-version
```

## Usage

Create a `BucketClass` and `BucketClaim` to provision a bucket:

```yaml
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClass
metadata:
  name: backblaze
driverName: b2.backblaze.com
deletionPolicy: Delete

---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClaim
metadata:
  name: my-bucket
  namespace: default
spec:
  bucketClassName: backblaze
  protocols:
    - s3
```

Grant access to the bucket:

```yaml
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccessClass
metadata:
  name: backblaze
driverName: b2.backblaze.com
authenticationType: Key

---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccess
metadata:
  name: my-bucket-access
  namespace: default
spec:
  bucketClaimName: my-bucket
  bucketAccessClassName: backblaze
  credentialsSecretName: my-bucket-credentials
```

The driver will create a scoped B2 application key and store S3-compatible credentials in the `my-bucket-credentials` secret.

## Parameters

### `BucketClass.parameters`

| Key              | Values     | Effect                                                                                                                                                                                       |
| ---------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bucketType`     | `public`   | Create the bucket as public. Defaults to private.                                                                                                                                            |
| `lifecycleRules` | JSON array | B2 lifecycle rules. Each element accepts `fileNamePrefix`, `daysFromUploadingToHiding`, `daysFromHidingToDeleting` — field names match the [B2 API](https://www.backblaze.com/apidocs/b2-update-bucket). At least one `days*` field must be > 0. |

Lifecycle rules are applied only at bucket creation; editing the `BucketClass` after a bucket is Ready has no effect.

The B2 dashboard's lifecycle radio buttons map onto `lifecycleRules` like this:

| Dashboard option                        | `lifecycleRules` value                                                                          |
| --------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Keep all versions (default)             | _omit the parameter_                                                                            |
| Keep only the last version              | `[{"daysFromHidingToDeleting": 1}]`                                                             |
| Keep prior versions for *N* days        | `[{"daysFromHidingToDeleting": N}]`                                                             |
| Custom                                  | _the same JSON the B2 API expects_                                                              |

### `BucketAccessClass.parameters`

| Key          | Values         | Effect                                                                                |
| ------------ | -------------- | ------------------------------------------------------------------------------------- |
| `accessMode` | `ro` / `read`  | Mint a read-only application key. Defaults to read-write.                              |
