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
kubectl apply -f https://raw.githubusercontent.com/bo0tzz/b2-cosi-driver/v0.0.1/config/rbac.yaml
kubectl apply -f https://raw.githubusercontent.com/bo0tzz/b2-cosi-driver/v0.0.1/config/deployment.yaml
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
